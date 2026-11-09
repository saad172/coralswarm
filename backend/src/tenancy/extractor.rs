//! The `Tenant` axum extractor (org-tenancy PR 1.4).
//!
//! Route files historically reached for `state.db` / `state.store` directly —
//! fine pre-split, when both were always the single shared Postgres instance.
//! Post-split, a request bound to an org (`TenancyDecision::Org`) must never
//! touch that ambient consumer pool for CONTENT-plane work: its atoms/graphs/
//! queries/etc. live only in the org's own tenant database. `Tenant` is the
//! one place a handler resolves which pool/store it should actually use, so
//! route bodies stop asking `AppState` directly for content-plane access
//! ("no ambient pool" — the plan's PR 1.6 grep-guard enforces this later;
//! this extractor is what lets each route conform to it *before* the guard
//! lands, chunk by chunk).
//!
//! Design note — **this extractor does not exist upstream.** The org-tenancy
//! implementation plan describes it as landing in PR 1.2, but the PR 1.2 that
//! actually merged (binding gate + `TenancyDecision` stamping only) never
//! added it; grepping `main` for `FromRequestParts`/`struct Tenant` before
//! this PR turns up nothing. It is introduced here, in PR 1.4, as the
//! mechanical seam the five files in this chunk (and 1.5/1.6 after it) need.
//!
//! Resolution rule, matching the convention [`crate::tenancy::reachability::
//! ReachabilityResolver`] already established for its own `Option<&
//! TenancyDecision>` parameter — with one FLAG-CONDITIONAL exception the
//! reachability resolver doesn't need (it's never the FIRST thing to see a
//! request; this extractor is):
//!
//! * **No `TenancyDecision` extension present, AND the tenancy runtime is
//!   disabled or absent** (`MULTITENANT_ENABLED` unset, the default, or no
//!   `TENANT_APEX_DOMAIN` configured) — the NORMAL, default, flag-off state
//!   for every request in every deployment that hasn't turned multitenancy
//!   on. Treated IDENTICALLY to [`TenancyDecision::Personal`]: legacy
//!   `state.db`/`state.store`. Byte-identical to pre-1.4 code.
//! * **No `TenancyDecision` extension present, but the tenancy runtime IS
//!   enabled** — `middleware::auth::require_auth` stamps a `TenancyDecision`
//!   (Personal *or* Org — see its own doc comment) on **every** request it
//!   processes once `state.tenant_runtime.enabled`, so an enabled-but-absent
//!   decision can only mean this route handler is mounted OUTSIDE the gate
//!   layer — a real deploy-bug class (a route wired onto the wrong router, or
//!   layered before the gate middleware). Silently falling back to Personal
//!   here would be exactly the cross-plane leak this whole extractor exists
//!   to prevent: an org-subdomain request would quietly read/write the
//!   shared CONSUMER plane instead of 404ing or routing to its tenant pool.
//!   So this arm fails loud: a 500 (logged at `error!`, a signal Fly/log-based
//!   alerting can page on — see the plan's "gate 403/404 spikes" alerting
//!   item), never a served response. This is the ONE arm that differs from
//!   the "absent ⇒ Personal" convention above, and it's why `Rejection` is
//!   `Response`, not `Infallible`: this extractor must be able to refuse.
//! * **`TenancyDecision::Personal`** — same `state.db`/`state.store` handles,
//!   so identity/behavior is unchanged from pre-1.4 code.
//! * **`TenancyDecision::Org(ctx)`** — a fresh [`PostgresStore`] wrapping
//!   `ctx.pool` (the fingerprint-verified tenant pool) plus that same pool.
//!   `PostgresStore::new` is a cheap wrapper (one `PgPool` clone, no I/O), so
//!   building one per request is not a meaningful cost.
//!
//! **Control-plane note:** `Tenant` only ever resolves CONTENT-plane handles.
//! Some `Store` methods (`account_is_org`, `graph_reachable_by_user`,
//! `list_reachable_graphs`) and a few free functions read control-only
//! tables (`accounts`,
//! `account_members`, `account_prefs`) that do not exist on a tenant
//! database; those call sites must keep using `state.db`/`state.store` (or,
//! for reachability, [`crate::tenancy::reachability::ReachabilityResolver`],
//! which already knows how to split the query) — `Tenant` does not attempt
//! to hide that distinction, since blindly routing a control-table read at a
//! tenant pool would be a `relation "accounts" does not exist` failure at
//! runtime, not a compile-time error.

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::route_class::RouteClass;
use crate::store::{PostgresStore, Store};
use crate::AppState;

use super::runtime::{TenancyDecision, TenantContext};

/// Tenant-scoped content-plane handles, resolved once per request from the
/// binding gate's `TenancyDecision` (see the module doc for the resolution
/// rule). Cloning is cheap (`Arc`/`PgPool` clones only).
#[derive(Clone)]
pub struct Tenant {
    /// The content-plane `Store`. `None` only when neither a tenant pool nor
    /// a legacy `state.store` is configured (e.g. tests, or a
    /// misconfigured cloud deploy) — handlers keep their own
    /// `require_store!`-style 503 handling for that case.
    pub store: Option<Arc<dyn Store>>,
    /// The raw pool backing `store` — `state.db` on Personal, the tenant pool
    /// on Org. Needed by call sites that haven't (yet, or ever will) move
    /// behind the `Store` trait: activity-log inserts, R2-blob DEK
    /// resolution, job-queue enqueue.
    pub pool: Option<sqlx::PgPool>,
    /// The resolved decision, `None` meaning "gate didn't run / Personal"
    /// (see module doc). Kept as the request's own `TenancyDecision` (not
    /// collapsed to `Personal`) so callers that need to tell "gate ran and
    /// chose personal" apart from "gate never ran" still can — and so it
    /// threads directly into [`crate::tenancy::reachability::
    /// ReachabilityResolver`] and [`crate::crypto::resolve_dek_cached_dispatch`]
    /// without another layer of translation.
    pub decision: Option<TenancyDecision>,
    /// The route class this request runs under
    /// (`docs/plans/page-load-standard/design.md` §4). On `Org` it selects
    /// the lane `pool`/`store` (and `decision`'s own `ctx.pool`) point at;
    /// on every plane it selects the transaction shape
    /// `begin_member_scoped` opens (read-only and 1 s on `PageRead`).
    pub class: RouteClass,
}

impl Tenant {
    /// The org `TenantContext`, if this request is scoped to one.
    pub fn org_context(&self) -> Option<&TenantContext> {
        match &self.decision {
            Some(TenancyDecision::Org(ctx)) => Some(ctx),
            _ => None,
        }
    }

    /// Build the legacy/Personal-equivalent `Tenant` directly from
    /// `AppState`, for call sites that resolve shared, in-process logic
    /// outside axum's own request extraction. Identical to what
    /// [`FromRequestParts::from_request_parts`] produces when no
    /// `TenancyDecision` extension is present AND the tenancy runtime is
    /// disabled, so a caller that later gains real gate wiring can switch to
    /// the axum extractor with no behavior change for the flag-off/Personal
    /// case.
    ///
    /// **This constructor BYPASSES the axum extractor's flag-conditional
    /// "absent decision + runtime enabled ⇒ 500" tripwire by construction —
    /// it never even looks at `state.tenant_runtime`.** It must never be
    /// called from a request path that a request carrying a real
    /// `TenancyDecision::Org` could reach once `MULTITENANT_ENABLED` is on
    /// (that would silently serve org-subdomain traffic from the consumer
    /// plane, exactly the failure mode the tripwire exists to catch). The MCP
    /// tools no longer use this at all (org-tenancy PR 2.3: `mcp_auth`
    /// resolves a claim-only `TenancyDecision` and the tools build their
    /// handles via `mcp::tools::content_tenant`), and the connector/
    /// teams_ingest funnels no longer use it either (org-tenancy PR 1.6:
    /// `routes::connectors` threads the real `Tenant` behind a fail-closed
    /// org gate; `routes::teams_ingest` moved onto the same). The remaining
    /// call sites, current as of PR 1.6:
    /// * the signature/subscription-authenticated webhook ingest funnels
    ///   (`routes::graph_webhook`) — per-user consumer-plane paths that carry
    ///   no Clerk org context by construction (the plan's PR 1.6 allowlist);
    /// * the PUBLIC share viewers (`routes::shares::public_share`,
    ///   `routes::session_shares::public_session_share`) — open-router
    ///   routes with no auth layer, so no `TenancyDecision` ever exists
    ///   there; consumer-plane token resolution is their DESIGNED behavior
    ///   (an org-minted token simply 404s — fail closed, see their doc
    ///   comments).
    ///
    /// Do not reach for `Tenant::legacy` in new code without confirming the
    /// call path is genuinely unreachable under an Org decision.
    pub fn legacy(state: &AppState) -> Self {
        Tenant {
            store: state.store.clone(),
            pool: state.db.clone(),
            decision: None,
            class: RouteClass::Other,
        }
    }
}

impl FromRequestParts<Arc<AppState>> for Tenant {
    /// Not `Infallible`: the flag-conditional deploy-bug tripwire (see the
    /// module doc's "absent decision, runtime enabled" arm) means this
    /// extractor CAN refuse a request outright — a plain `Response` (a 500)
    /// rather than a typed rejection, since there is nothing a caller could
    /// usefully pattern-match on here (see `middleware::auth`'s own 503/401
    /// responses for the same "just build the Response inline" convention).
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let decision = parts.extensions.get::<TenancyDecision>().cloned();
        let class = RouteClass::of(&parts.method, &parts.extensions);
        // Free (already-resolved `Arc` field reads, no I/O): whether the
        // binding gate is actually enabled for this deploy. When it's off (or
        // the runtime was never constructed — most tests), an absent
        // decision is the normal flag-off state; when it's ON, the gate (see
        // `middleware::auth::require_auth`'s own doc comment) stamps a
        // `TenancyDecision` on EVERY request it processes, so absence here
        // can only mean this handler is mounted outside the gate layer.
        let runtime_enabled = state.tenant_runtime.as_ref().is_some_and(|rt| rt.enabled);

        match &decision {
            None if runtime_enabled => {
                // Deploy-bug tripwire (org-tenancy PR 1.4 P1 fix): a route
                // reachable with the binding gate enabled, but not wired
                // behind it, would otherwise silently resolve to the
                // CONSUMER plane here — indistinguishable from a legitimate
                // Personal request. Fail loud instead: logged at `error!` so
                // log-based alerting (the plan's "gate 403/404 spikes"
                // monitoring item) can page on it, and refused with a plain
                // 500 rather than ever serving from the wrong plane.
                tracing::error!(
                    path = %parts.uri.path(),
                    "Tenant extractor: no TenancyDecision present but the tenancy runtime is \
                     ENABLED — this route is mounted outside the binding gate layer (refusing \
                     rather than silently falling back to the consumer plane)"
                );
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response())
            }
            None | Some(TenancyDecision::Personal) => Ok(Tenant {
                store: state.store.clone(),
                pool: state.db.clone(),
                decision,
                class,
            }),
            Some(TenancyDecision::Org(ctx)) => {
                // Lane selection (design §4.3). A page read gets a context
                // whose `pool` IS the page lane, so the store, the raw pool
                // and anything that later reads `ctx.pool` off `decision`
                // all agree on the lane.
                let ctx = match (class, &ctx.page_pool) {
                    (RouteClass::PageRead, Some(page)) => TenantContext {
                        pool: page.clone(),
                        ..ctx.clone()
                    },
                    _ => ctx.clone(),
                };
                let store: Arc<dyn Store> = Arc::new(PostgresStore::new(ctx.pool.clone()));
                let pool = ctx.pool.clone();
                Ok(Tenant {
                    store: Some(store),
                    pool: Some(pool),
                    decision: Some(TenancyDecision::Org(ctx)),
                    class,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenancy::registry::TenantRegistry;
    use crate::tenancy::runtime::TenantRuntime;
    use crate::tenancy::secrets::StaticMapResolver;
    use axum::{body::Body, http::Request as HttpRequest, routing::get, Extension, Router};
    use tower::ServiceExt;

    fn state_no_db() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn runtime(enabled: bool) -> Arc<TenantRuntime> {
        let resolver = StaticMapResolver::from_pairs(Vec::<(String, String)>::new());
        TenantRuntime::new(enabled, "example.com", Arc::new(TenantRegistry::new(Arc::new(resolver))))
    }

    async fn probe_handler(tenant: Tenant) -> axum::http::StatusCode {
        // Absent decision (gate never ran / flag off) resolves to the legacy
        // handles rather than rejecting — this is the whole point of the
        // module doc's "flag-off / disabled runtime" note.
        assert!(tenant.org_context().is_none());
        axum::http::StatusCode::OK
    }

    #[tokio::test]
    async fn absent_decision_resolves_to_legacy_handles_not_rejection() {
        let state = state_no_db();
        let router = Router::new()
            .route("/probe", get(probe_handler))
            .with_state(state);
        let req = HttpRequest::builder()
            .uri("/probe")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// Same "absent decision" input, but with a `TenantRuntime` PRESENT and
    /// explicitly `enabled: false` (rather than `tenant_runtime: None`) —
    /// covers the module doc's "disabled OR absent" phrasing's other half.
    /// Still legacy handles, still 200.
    #[tokio::test]
    async fn absent_decision_with_disabled_runtime_still_resolves_to_legacy_handles() {
        let mut state = (*state_no_db()).clone();
        state.tenant_runtime = Some(runtime(false));
        let router = Router::new()
            .route("/probe", get(probe_handler))
            .with_state(Arc::new(state));
        let req = HttpRequest::builder()
            .uri("/probe")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// The P1 fix, verified end-to-end: a route mounted WITHOUT the binding
    /// gate layer (so no `TenancyDecision` extension is ever stamped) but
    /// with the tenancy runtime ENABLED must hard-500, never silently serve
    /// from the consumer plane. This is exactly the failure mode a future
    /// route migrated to `Tenant` but mounted on the wrong router (e.g. the
    /// `open` router `routes/shares.rs`/`routes/session_shares.rs` live on
    /// today) would hit.
    #[tokio::test]
    async fn enabled_runtime_with_absent_decision_hard_500s_route_mounted_outside_gate() {
        let mut state = (*state_no_db()).clone();
        state.tenant_runtime = Some(runtime(true));
        // No `require_auth`/binding-gate middleware layered here at all —
        // simulates a handler wired onto a router that never ran the gate.
        let router = Router::new()
            .route("/probe", get(probe_handler))
            .with_state(Arc::new(state));
        let req = HttpRequest::builder()
            .uri("/probe")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "an enabled runtime + absent TenancyDecision must hard-500, never fall back to Personal"
        );
    }

    #[tokio::test]
    async fn personal_decision_resolves_to_legacy_handles() {
        let state = state_no_db();
        async fn handler(tenant: Tenant) -> axum::http::StatusCode {
            assert!(tenant.org_context().is_none());
            assert!(matches!(tenant.decision, Some(TenancyDecision::Personal)));
            axum::http::StatusCode::OK
        }
        let router = Router::new()
            .route("/probe", get(handler))
            .layer(Extension(TenancyDecision::Personal))
            .with_state(state);
        let req = HttpRequest::builder()
            .uri("/probe")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn org_decision_builds_a_fresh_store_over_the_tenant_pool() {
        use uuid::Uuid;
        let state = state_no_db();
        let ctx = TenantContext {
            org_id: "org_a".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects"),
            page_pool: None,
        };
        async fn handler(tenant: Tenant) -> axum::http::StatusCode {
            assert!(tenant.org_context().is_some());
            assert!(tenant.store.is_some());
            assert!(tenant.pool.is_some());
            assert_eq!(tenant.class, RouteClass::Other);
            axum::http::StatusCode::OK
        }
        let router = Router::new()
            .route("/probe", get(handler))
            .layer(Extension(TenancyDecision::Org(ctx)))
            .with_state(state);
        let req = HttpRequest::builder()
            .uri("/probe")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// Two lazily-built pools with different `max_connections`, so the lane
    /// a handler received is observable without connecting to anything.
    fn two_lane_ctx() -> TenantContext {
        use uuid::Uuid;
        let lazy = |max: u32| {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(max)
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects")
        };
        TenantContext {
            org_id: "org_lanes".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            pool: lazy(3),
            page_pool: Some(lazy(2)),
        }
    }

    async fn lane_max(tenant: Tenant) -> String {
        let pool_max = tenant
            .pool
            .as_ref()
            .unwrap()
            .options()
            .get_max_connections();
        let ctx_max = tenant
            .org_context()
            .unwrap()
            .pool
            .options()
            .get_max_connections();
        format!("{}:{pool_max}:{ctx_max}", tenant.class.as_str())
    }

    async fn body_of(router: Router, uri: &str) -> String {
        let resp = router
            .oneshot(HttpRequest::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        String::from_utf8(
            axum::body::to_bytes(resp.into_body(), 1 << 16)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_page_read_request_runs_on_the_page_lane_and_other_on_the_work_lane() {
        let state = state_no_db();
        let page = Router::new()
            .route("/page", get(lane_max))
            .layer(Extension(RouteClass::PageRead));
        let other = Router::new().route("/other", get(lane_max));
        let router = page
            .merge(other)
            .layer(Extension(TenancyDecision::Org(two_lane_ctx())))
            .with_state(state);
        // `pool`, `store`'s pool and `decision`'s `ctx.pool` are the same
        // handle on both classes: the lane is chosen once.
        assert_eq!(body_of(router.clone(), "/page").await, "page_read:2:2");
        assert_eq!(body_of(router, "/other").await, "other:3:3");
    }

    #[tokio::test]
    async fn a_single_lane_context_serves_page_reads_from_its_one_pool() {
        let state = state_no_db();
        let ctx = TenantContext {
            page_pool: None,
            ..two_lane_ctx()
        };
        let router = Router::new()
            .route("/page", get(lane_max))
            .layer(Extension(RouteClass::PageRead))
            .layer(Extension(TenancyDecision::Org(ctx)))
            .with_state(state);
        assert_eq!(body_of(router, "/page").await, "page_read:3:3");
    }
}
