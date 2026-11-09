//! The admin surface for D16's connector-health record (CS-446,
//! org-onboarding plan §3 item 16, `docs/plans/org-onboarding-from-the-
//! invite.md`):
//!
//! - `GET /v1/orgs/connector-health` — admin: every CURRENT connector
//!   degradation for this org, re-checked at read time, plus (CS-448) the
//!   read-time attribution for a rung-1 directory nobody is backing.
//!
//! One route, deliberately — and still one after CS-448. That item needed a
//! second thing said on the same screen, to the same admin, about the same
//! capability; a second endpoint would have meant a second fetch, a second
//! admin gate and two answers that can disagree about whether this org has
//! a directory. So the response grew from a bare array to an object with
//! two members (`degradations`, `directory_attribution`) rather than the
//! route set growing. The attribution is NOT a degradation and is
//! deliberately not shaped like one — see [`DirectoryAttribution`].
//!
//! One route, deliberately. There is nothing for an admin to POST here: a
//! degradation is not dismissed, acknowledged or snoozed — it stops being
//! true when the capability comes back, and `services::connector_health::
//! refresh_open_degradations` re-derives that on every read rather than
//! trusting a stored flag. A "dismiss" button on a condition that is still
//! happening is how a real signal becomes one people learn to click past;
//! D13's own "an empty digest trains people to ignore the full one" is the
//! same rule from the other side.
//!
//! Admin-gated ([`Tenant`] + `require_org_admin`) and personal-orgs
//! refused, exactly like `routes::org_join_requests`' own admin routes and
//! `routes::org_slots`'. The degradation names WHOSE credential the org was
//! resting on — a departed colleague's address — which is admin
//! information, and the setting it affects (auto-add's strength) is an
//! admin's setting.
//!
//! **This is not the `connector_health = true` tracing field.** See
//! `services::connector_health`'s own module doc for why the two share a
//! name and are different things; the short version is that a `warn!` is
//! visible to an operator tailing logs and to nobody else, which is the
//! "silently" D16's build note is about.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use utoipa::path as utoipa_path;
use utoipa::ToSchema;

use crate::middleware::auth::UserContext;
use crate::services::connector_health as svc;
use crate::tenancy::extractor::Tenant;
use crate::AppState;

use super::org_invites::require_org_admin;

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}
fn forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}
fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "connector_health: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

/// Same bar `routes::org_slots::refuse_personal_org` and
/// `routes::org_join_requests::refuse_personal_org` apply to their own
/// admin actions, duplicated here for the same stated reason those two give
/// for duplicating it: it is eight lines, and widening either of theirs to
/// `pub(crate)` to share it would couple three route modules for no gain. A
/// personal org has one member who never departs and no directory concept
/// at all, so it has no connector health to report.
async fn refuse_personal_org(
    state: &AppState,
    ctx: &crate::tenancy::runtime::TenantContext,
) -> Result<(), Response> {
    let Some(store) = state.control_store() else {
        return Err(unavailable());
    };
    match store.account_is_real_org(ctx.account_id, ctx.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(forbidden("personal accounts have no connector health to report")),
        Err(e) => Err(internal("account lookup", e)),
    }
}

/// One CURRENT degradation, as the connectors page renders it.
///
/// `provider`/`capability`/`condition` are the three machine-readable keys
/// the web's own `connectorHealthModel` turns into copy — deliberately NOT
/// a pre-rendered sentence from the server. Wording belongs beside the
/// component that shows it (the same split `reconsentModel.ts` already
/// makes against `missing_scopes`), and a server that shipped prose would
/// make every copy change a backend deploy.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectorHealthItem {
    /// The `oauth_tokens.provider` literal — `"google_chat"` today.
    pub provider: String,
    /// Which capability of that connector degraded —
    /// `"domain_directory"` is D16's rung 1.
    pub capability: String,
    /// Why — `"backing_member_departed"` today. See migration 0243's
    /// `CHECK` for the full (currently one-value) domain.
    pub condition: String,
    /// The address of the member whose credential backed the capability,
    /// captured at the moment they left. This is the part that makes the
    /// notice actionable rather than merely true — "your directory lookup
    /// stopped working" is not something an admin can act on; "it was
    /// Dana's Google grant" is.
    pub backing_email: Option<String>,
    /// When the condition was first observed (or, after a resolution and a
    /// relapse, when the CURRENT episode began — never the previous one's
    /// start).
    #[schema(value_type = String, format = DateTime)]
    pub first_observed_at: chrono::DateTime<chrono::Utc>,
    /// When it was last confirmed still true. Advances on every admin read,
    /// which is what makes "still current" re-derived rather than stored.
    #[schema(value_type = String, format = DateTime)]
    pub last_observed_at: chrono::DateTime<chrono::Utc>,
    /// Free-form context (0243's `detail`) — rendered, never branched on.
    pub detail: JsonValue,
}

/// CS-448 — **attribution, not a degradation.**
///
/// The org has no `directory.readonly` backing at all, and these still-
/// present members disconnected Google Chat and have not reconnected. The
/// point of this payload is to let one notice say, in one place, that those
/// two facts are the same fact: item 49 already shows an admin "this member
/// hasn’t connected a tool" and the ladder already quietly drops to rung
/// 2, and until now nothing joined them.
///
/// # Why this is not a `ConnectorHealthItem`
///
/// It has no `condition`, no `first_observed_at` and no `last_observed_at`,
/// because it is not a recorded episode with a lifecycle — it is a fact
/// re-derived on every read from `oauth_tokens`, `account_members` and
/// `connector_audit_log`. Giving it the row shape would have meant giving
/// it a `condition` string, and migration 0243’s `CHECK` admits exactly one
/// of those on purpose: *"a writer for a new condition has to add the value
/// here, in its own migration, with its own reasoning."* CS-448 has no
/// writer and no migration, so it takes no value from that domain, not even
/// on the wire. `provider`/`capability` ARE shared, because they are the
/// keys that tie both payloads to the same connector tile.
#[derive(Debug, Serialize, ToSchema)]
pub struct DirectoryAttribution {
    /// `"google_chat"` — the same tile the member would act on.
    pub provider: String,
    /// `"domain_directory"` — D16’s rung 1.
    pub capability: String,
    /// Newest disconnect first. Never empty: an org that simply never had a
    /// directory (every Slack-only org) produces no attribution at all
    /// rather than an empty one, so a client can render on truthiness.
    pub members: Vec<DisconnectedMember>,
}

/// One still-present member named by a [`DirectoryAttribution`].
///
/// `email`, not `user_id`, for the reason [`ConnectorHealthItem`]’s own
/// `From` impl gives: the address is what an admin recognises, and nothing
/// on this surface addresses the person programmatically.
#[derive(Debug, Serialize, ToSchema)]
pub struct DisconnectedMember {
    pub email: String,
    /// From `connector_audit_log` (migration 0193) — read, never written.
    #[schema(value_type = String, format = DateTime)]
    pub disconnected_at: chrono::DateTime<chrono::Utc>,
}

/// The whole read: what is recorded, and what is merely derived.
///
/// Two members rather than one array because they are different kinds of
/// claim and must stay tellable apart — `degradations` are durable rows
/// with a lifecycle an admin can watch resolve, `directory_attribution` is
/// a view of right now that exists only for as long as the read.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectorHealthResponse {
    /// CURRENT degradations, oldest first. Empty when nothing is recorded.
    pub degradations: Vec<ConnectorHealthItem>,
    /// Present only when the org definitely has no rung-1 backing AND a
    /// still-present member’s Google Chat disconnect is on the audit log.
    /// `None` covers all three of: somebody backs it, we cannot tell, and
    /// nobody ever did — which the surface renders identically (nothing),
    /// so it has no reason to tell them apart.
    pub directory_attribution: Option<DirectoryAttribution>,
}

impl From<svc::Degradation> for ConnectorHealthItem {
    fn from(d: svc::Degradation) -> Self {
        // `backing_user_id` is deliberately NOT exposed: it is an internal
        // `users.id` with no meaning to a browser, and the address is the
        // thing an admin recognises. Nothing on the surface needs to
        // address that person programmatically.
        Self {
            provider: d.provider,
            capability: d.capability,
            condition: d.condition,
            backing_email: d.backing_email,
            first_observed_at: d.first_observed_at,
            last_observed_at: d.last_observed_at,
            detail: d.detail,
        }
    }
}

#[utoipa_path(
    get,
    path = "/v1/orgs/connector-health",
    tag = "connector_health",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Current connector degradations for this org (oldest first), plus the read-time attribution for a rung-1 directory nobody is backing", body = ConnectorHealthResponse),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    // `refresh_open_degradations`, not a bare `list_open`: a degradation an
    // admin has already fixed (by connecting their own Google Chat with
    // `directory.readonly` — the remedy the notice itself names) must stop
    // being shown the moment they look, and the refresh is what closes it
    // without a second write hook in the OAuth callback. It can resolve and
    // it can re-confirm; it structurally cannot OPEN a condition, which is
    // what keeps "this org has no directory" (every Slack-only org) from
    // ever becoming "this org LOST its directory".
    let degradations: Vec<ConnectorHealthItem> =
        match svc::refresh_open_degradations(control, tenant.pool.as_ref(), ctx.account_id).await {
            Ok(rows) => rows.into_iter().map(Into::into).collect(),
            Err(e) => return internal("list connector health", e),
        };

    // CS-448. A pure read (`svc::directory_backing` takes a pool and an
    // account id — no HTTP client, no directory-fetch result, nothing that
    // could turn a transient into a claim), joining who backs rung 1 now to
    // who disconnected and stayed. Runs on every read for the same reason
    // `refresh_open_degradations` does: the moment somebody reconnects, the
    // attribution stops being true, and re-deriving it is what makes that
    // automatic instead of another write hook in the OAuth callback.
    //
    // `Backed`, `Indeterminate` and an `Unbacked` with nobody to name all
    // collapse to `None`. That last one is the common case and the
    // important one: every Slack-only org is permanently unbacked and must
    // never be told it lost something it never had.
    let directory_attribution = match svc::directory_backing(control, ctx.account_id).await {
        Ok(svc::DirectoryBacking::Unbacked { disconnected }) if !disconnected.is_empty() => {
            Some(DirectoryAttribution {
                provider: svc::PROVIDER_GOOGLE_CHAT.to_string(),
                capability: svc::CAPABILITY_DOMAIN_DIRECTORY.to_string(),
                members: disconnected
                    .into_iter()
                    .map(|m| DisconnectedMember {
                        email: m.email,
                        disconnected_at: m.disconnected_at,
                    })
                    .collect(),
            })
        }
        Ok(_) => None,
        // Deliberately non-fatal, and the ONLY difference in posture
        // between the two halves of this response. `degradations` is the
        // recorded half and a failure there is a 500; the attribution is a
        // derived nicety on top of it, and dropping the whole notice
        // because a supporting read failed would hide the durable
        // degradation an admin actually needs to see. Logged at `error!`,
        // not `warn!`, so it is not silent to an operator either.
        Err(e) => {
            tracing::error!(
                error = %e,
                account_id = %ctx.account_id,
                step = "directory attribution",
                "connector_health: attribution read failed — serving degradations without it"
            );
            None
        }
    };

    (
        StatusCode::OK,
        Json(ConnectorHealthResponse { degradations, directory_attribution }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use utoipa::OpenApi;

    /// CS-448 changed this route's 200 body from a bare
    /// `[ConnectorHealthItem]` to [`super::ConnectorHealthResponse`], and
    /// did it WITHOUT editing `openapi.rs` — utoipa collects a `ToSchema`
    /// type reachable from an already-registered `#[utoipa::path]`, so the
    /// new envelope and both of its nested types ride in on
    /// `connector_health::list`'s existing registration.
    ///
    /// That is convenient and it is also exactly the shape of the gap
    /// `openapi.rs`'s own `rename_team_request_body_is_registered_as_a
    /// _schema` test was written for: a body type that IS the contract but
    /// is only present in the spec by a mechanism nobody restated. Pinned
    /// here rather than assumed, so a future refactor that drops the
    /// `body = ` annotation, or moves the type behind a `serde_json::Value`,
    /// fails a test instead of silently emptying the published contract.
    #[test]
    fn the_response_envelope_and_its_parts_are_in_the_published_spec() {
        let doc = crate::openapi::ApiDoc::openapi();
        let components = doc.components.expect("ApiDoc must have components");
        for name in [
            "ConnectorHealthResponse",
            "ConnectorHealthItem",
            "DirectoryAttribution",
            "DisconnectedMember",
        ] {
            assert!(
                components.schemas.contains_key(name),
                "{name} must be registered under #/components/schemas (present: {:?})",
                components.schemas.keys().collect::<Vec<_>>()
            );
        }
    }
}
