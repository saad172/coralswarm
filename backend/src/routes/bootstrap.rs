//! `GET /v1/bootstrap` — everything the web app shell needs before first
//! paint, in one response.
//!
//! The shell used to make these reads one by one after Clerk handed it a
//! bearer: `GET /v1/me` and `GET /v1/graphs` gate `ready` (web/lib/
//! providers.tsx `attemptBoot`), and `GET /v1/orgs/join-requests/count` fills
//! the sidebar's Settings badge on every page. Each is its own round trip
//! through auth. This route authenticates ONCE (the request's own
//! `require_auth` pass, #1442's single control-plane read) and runs the three
//! handlers concurrently on the same `Tenant` and `UserContext`.
//!
//! # Each part IS its endpoint's body
//!
//! The parts are produced by calling the existing handlers
//! ([`me::get_me`], [`graphs::list_graphs`], [`org_join_requests::count`]),
//! not by re-deriving their payloads, so a field added to `/v1/me` lands here
//! with no edit. `tests/bootstrap_pg.rs` pins byte equality against each
//! standalone endpoint for an org admin, an org member and a personal user.
//!
//! # Trial-only opt-in
//!
//! `?billing_banner=trial_v1` replaces the legacy `billing_banner` full
//! summary with `billing_banner_mode: "trial_v1"` and `trial_banner`, a
//! distinct DB-only DTO. No provider lookup is reachable in that branch.
//! Absent/unknown modes keep the complete legacy body for old browsers.
//!
//! Journal selection flags `activities_enabled` and `gist_enabled` use the
//! same helpers as their model gates and require no additional I/O.
//!
//! Plus one flag: `selfserve_signup_enabled` (`ORG_SELFSERVE_SIGNUP_ENABLED`,
//! self-serve signup A2), so the web shows "create an organization" only
//! when `POST /v1/orgs` is on.
//!
//! And `pending_orgs` (#1540): the caller's self-serve orgs whose workspace is
//! not ready, `[{"org_id": "org_…", "status": "awaiting_payment"}]`, so the
//! web can keep them out of Clerk's org switcher. `null` when unreadable.
//!
//! And `selfserve_org` (#1540 review SF-a): whether the caller's ACTIVE
//! account is a team org made by `POST /v1/orgs`. The People page mounts
//! Clerk's `<OrganizationProfile/>` only when this is `false`. `null` when
//! unreadable.
//!
//! Both are read inside the same `tokio::join!` as the handlers, each under
//! [`OPTIONAL_PART_DEADLINE`]: past it the part is `null` (logged), so a
//! slow control-plane read is bounded. The join still waits for every part.
//!
//! # What is NOT here
//!
//! * `PUT /v1/me/timezone` is a fire-and-forget WRITE the client sends after
//!   boot; nothing renders from it and a GET must not write, so it stays its
//!   own request off the critical path.
//! * Per-page data (conversations, library scope, ...) belongs to the page.
//!
//! # Failure semantics
//!
//! * `me` or `graphs` not 2xx: the whole response is that part's status and
//!   body, exactly what the standalone call would have answered. The shell
//!   cannot render without either, which is what it does today.
//! * `join_requests_count` is optional. A 4xx is the endpoint's normal answer
//!   for a caller with no queue (personal account, personal org, non-admin
//!   member) and becomes `null` silently. A 5xx or an unreadable body also
//!   becomes `null`; the bounded completion record carries its outcome. The
//!   response is still 200.
//!
//! The three futures share one task (`tokio::join!`, no `spawn`), so the
//! request's task-locals, including `request_timing`'s `Server-Timing`
//! accumulator, see every transaction the parts open.

use std::{future::Future, sync::Arc};

pub(crate) mod measurement;
use measurement::{Guard, Outcome, Phase};

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde_json::{json, Value};
use utoipa::path as utoipa_path;

use crate::middleware::auth::UserContext;
use crate::routes::{graphs, me, org_join_requests};
use crate::tenancy::extractor::Tenant;
use crate::AppState;

/// The count endpoint's body is `{"open": n}`; far below this.
const OPTIONAL_BODY_LIMIT: usize = 256 * 1024;

/// How long each of the four deadline-wrapped optional parts may take before it is
/// answered `null` (#1540 review SF-f).
pub(crate) const OPTIONAL_PART_DEADLINE: std::time::Duration =
    std::time::Duration::from_millis(500);

/// Explicit opt-in: unknown/absent modes retain the legacy full-summary body.
#[derive(Default, serde::Deserialize)]
pub struct BootstrapQuery {
    pub billing_banner: Option<String>,
}

#[utoipa_path(
    get,
    path = "/v1/bootstrap",
    tag = "bootstrap",
    security(("bearer_token" = [])),
    params(("billing_banner" = Option<String>, Query, description = "trial_v1 opts into the distinct DB-only trial_banner DTO; absent/unknown retains the full legacy billing_banner")),
    responses(
        (status = 200, description = "`{ me, graphs, join_requests_count, selfserve_signup_enabled }`: the first three are the bodies of GET /v1/me, GET /v1/graphs and GET /v1/orgs/join-requests/count; `join_requests_count` is null when that endpoint does not answer 200; `selfserve_signup_enabled` says whether POST /v1/orgs is on; `pending_orgs` is `[{org_id, status}]`, the caller's self-serve orgs that are not ready yet (null if unreadable or slow); `selfserve_org` says whether the active account is a self-serve team org (null if unreadable or slow)", body = Value),
        (status = 403, description = "Same as GET /v1/me (e.g. a removed org member)"),
        (status = 500, description = "GET /v1/graphs failed"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_bootstrap(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(query): Query<BootstrapQuery>,
) -> Response {
    let trial_only = query.billing_banner.as_deref() == Some("trial_v1");
    let (response, record) = measurement::scope(&user.role, async {
        let parts = join_parts(
            me::get_me(
                State(state.clone()),
                tenant.clone(),
                Extension(user.clone()),
            ),
            graphs::list_graphs(tenant.clone(), Extension(user.clone())),
            org_join_requests::count(
                State(state.clone()),
                tenant.clone(),
                Extension(user.clone()),
            ),
            pending_orgs_part(&state, &user),
            selfserve_org_part(&state, &user),
            async {
                optional_named_part(
                    org_join_requests::list_mine(State(state.clone()), Extension(user.clone()))
                        .await,
                )
                .await
            },
            async {
                if crate::routes::org_invites::require_org_admin(&tenant, &user).is_err() {
                    return PartValue::null(Outcome::Skipped);
                }
                let response = if trial_only {
                    crate::routes::billing_manage::get_trial_banner(State(state.clone()), Extension(user.clone())).await
                } else {
                    crate::routes::billing_manage::get_billing_summary(State(state.clone()), Extension(user.clone())).await
                };
                optional_named_part(response).await
            },
        )
        .await;
        let response = assemble_mode(parts, trial_only).await;
        measurement::assembly_end(response.status());
        response
    })
    .await;
    measurement::emit(&record);
    response
}

type RequiredPart = (StatusCode, Json<Value>);
type Parts = (
    RequiredPart,
    RequiredPart,
    Response,
    Value,
    Value,
    Value,
    Value,
);

/// The production seven-way join is also the controlled-test seam. Keep the
/// unbounded count and four existing optional deadlines exactly where they are.
async fn join_parts(
    me: impl Future<Output = RequiredPart>,
    graphs: impl Future<Output = RequiredPart>,
    count: impl Future<Output = Response>,
    pending: impl Future<Output = PartValue>,
    selfserve: impl Future<Output = PartValue>,
    mine: impl Future<Output = PartValue>,
    billing: impl Future<Output = PartValue>,
) -> Parts {
    tokio::join!(
        required(Phase::Me, me),
        required(Phase::Graphs, graphs),
        async {
            let mut timing = Guard::start(Phase::AdminJoinCount);
            let response = count.await;
            timing.finish(Outcome::status(response.status()));
            response
        },
        within_deadline(Phase::PendingOrgs, pending),
        within_deadline(Phase::SelfserveOrg, selfserve),
        within_deadline(Phase::MyJoinRequests, mine),
        within_deadline(Phase::BillingBanner, billing),
    )
}

async fn required(phase: Phase, future: impl Future<Output = RequiredPart>) -> RequiredPart {
    let mut timing = Guard::start(phase);
    let result = future.await;
    timing.finish(Outcome::status(result.0));
    result
}

#[cfg(test)]
async fn assemble(parts: Parts) -> Response { assemble_mode(parts, false).await }

async fn assemble_mode(parts: Parts, trial_only: bool) -> Response {
    let (
        me_part,
        graphs_part,
        count_part,
        pending_orgs,
        selfserve_org,
        my_join_requests,
        billing_banner,
    ) = parts;
    measurement::assembly_start(me_part.0.is_success() && graphs_part.0.is_success());

    let (me_status, Json(me_body)) = me_part;
    if !me_status.is_success() {
        return (me_status, Json(me_body)).into_response();
    }
    let (graphs_status, Json(graphs_body)) = graphs_part;
    if !graphs_status.is_success() {
        return (graphs_status, Json(graphs_body)).into_response();
    }
    let count = optional_named_part(count_part).await;
    measurement::count_decoded(count.outcome);
    let join_requests_count = count.value;

    let mut body = json!({
            "me": me_body,
            "graphs": graphs_body,
            "join_requests_count": join_requests_count,
            // Self-serve signup A2 (for web PR B, #1534): whether `POST
            // /v1/orgs` and its status/retry routes are on
            // (`ORG_SELFSERVE_SIGNUP_ENABLED`), so the web need not probe.
            "selfserve_signup_enabled": crate::tenancy::provisioning::selfserve_signup_enabled(),
            // Direct section selection: the same gates as the stored models,
            // independently represented so activities-on/gist-off needs no probe.
            "activities_enabled": crate::services::journal_activities::enabled(),
            "gist_enabled": crate::services::journal_gist::enabled(),
            // #1540 (the gap web PR B left): the caller's self-serve orgs
            // whose workspace is not ready yet, `[{org_id, status}]`, so the
            // web can hide them in Clerk's org switcher or send the user to
            // their setup screen. `null` when it could not be read.
            "pending_orgs": pending_orgs,
            // #1540 review SF-a: whether the active account is a self-serve
            // team org (the People page keeps Clerk's own org profile for
            // every other org). `null` when it could not be read in time.
            "selfserve_org": selfserve_org,
            "my_join_requests": my_join_requests,
        });
    if trial_only {
        body["billing_banner_mode"] = json!("trial_v1");
        body["trial_banner"] = billing_banner;
    } else {
        body["billing_banner"] = billing_banner;
    }
    (StatusCode::OK, Json(body)).into_response()
}

/// [`crate::routes::orgs_create::pending_orgs_for`], or `null` (logged)
/// when the control plane is unavailable or the read fails. Optional like
/// `join_requests_count`: the shell renders without it.
async fn pending_orgs_part(state: &AppState, user: &UserContext) -> PartValue {
    let Some(control) = state.control_db() else {
        return PartValue::null(Outcome::Skipped);
    };
    match crate::routes::orgs_create::pending_orgs_for(control, user.user_id).await {
        Ok(list) => PartValue::ok(Value::Array(list)),
        Err(_) => PartValue::null(Outcome::Error),
    }
}

/// Unknown remains null: absence of a control database is not false.
async fn selfserve_org_part(state: &AppState, user: &UserContext) -> PartValue {
    let Some(control) = state.control_db() else {
        return PartValue::null(Outcome::Skipped);
    };
    match crate::routes::orgs_create::selfserve_org_for(control, user.account_id).await {
        Ok(b) => PartValue::ok(Value::Bool(b)),
        Err(_) => PartValue::null(Outcome::Error),
    }
}

struct PartValue {
    value: Value,
    outcome: Outcome,
}
impl PartValue {
    fn ok(value: Value) -> Self {
        Self {
            value,
            outcome: Outcome::Ok,
        }
    }
    fn null(outcome: Outcome) -> Self {
        Self {
            value: Value::Null,
            outcome,
        }
    }
}

/// Dropping the existing timeout future still cancels its read. Classification
/// comes from the executed branch/status, never a new eligibility query.
async fn within_deadline(phase: Phase, future: impl Future<Output = PartValue>) -> Value {
    let mut timing = Guard::start(phase);
    let result = match tokio::time::timeout(OPTIONAL_PART_DEADLINE, future).await {
        Ok(result) => result,
        Err(_) => PartValue::null(Outcome::Timeout),
    };
    timing.finish(result.outcome);
    result.value
}

/// Count body conversion remains AFTER the join and required refusals. The
/// four deadline-wrapped optional responses are decoded INSIDE their branch.
async fn optional_named_part(resp: Response) -> PartValue {
    let outcome = Outcome::status(resp.status());
    if outcome != Outcome::Ok {
        return PartValue::null(outcome);
    }
    let bytes = match axum::body::to_bytes(resp.into_body(), OPTIONAL_BODY_LIMIT).await {
        Ok(bytes) => bytes,
        Err(_) => return PartValue::null(Outcome::Error),
    };
    match serde_json::from_slice(&bytes) {
        Ok(value) => PartValue::ok(value),
        Err(_) => PartValue::null(Outcome::Error),
    }
}

#[cfg(test)]
mod tests;
