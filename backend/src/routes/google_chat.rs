//! Google Chat watched-space routes (gsuite Train G4 PRs 3-5 — `.scratch/
//! gsuite-enterprise/g4-chat-design.md` §3.2, §5.4, §5.5;
//! `docs/connectors/google-workspace.md` §9).
//!
//! Through gsuite Train G4 PR 5, every route here was personal-plane-only —
//! every handler refused outright under an org `TenancyDecision`, same
//! shape `routes::slack`'s own `reject_org_slack_management` uses for the
//! identical reason. **gsuite Train G4 PR 6 ("G4.6") lifts that gate behind
//! `ORG_GOOGLE_CHAT_ENABLED`** (mirrors the gsuite Train G2 PR 6 Gmail
//! org-plane lift, `docs/connectors/google-workspace.md` §2) — see
//! [`reject_org_google_chat_management`]'s own doc comment for the flag
//! resolution and [`reject_unauthorized_org_chat_destination_pick`]'s for the
//! member-scoping this lift REQUIRED closing first (security review finding
//! L9, below).
//!
//! Mailbox opt-ins now share the org flag gate. Credentials stay on
//! control; watched rows, cursors and the caller's member-private ocean stay
//! on the tenant database. Every pass checks current membership and uses
//! only the opting-in owner's token, without reader election or seating.
//!
//! * `GET /v1/connectors/google/chat/spaces` — [`list_spaces`]. Live
//!   `spaces.list` under the CALLING member's own Chat token, merged
//!   against the account's local `google_chat_watched_spaces` rows so the
//!   picker can show, per space: which archetype it is, whether the
//!   CoralSwarm app has ever been added (container archetype — a row exists
//!   at all, D6) or the calling member has personally opted in (mailbox
//!   archetype, G4.5), and where it currently routes.
//! * `POST /v1/connectors/google/chat/spaces/{space_id}/destination` —
//!   [`set_space_destination`]. The container-archetype destination pick
//!   (§3.2, I2) — the SPACE half of the picker `docs/connectors/
//!   slack-implementation.md`'s channel-management surface already
//!   established for Slack.
//! * `POST /v1/connectors/google/chat/spaces/{space_id}/optin` —
//!   [`opt_in_to_mailbox_space`] (G4.5). The MAILBOX archetype's opt-in
//!   (`spaceType ∈ {GROUP_CHAT, DIRECT_MESSAGE}`, contract §7): landing
//!   exclusively in the CALLING member's own private ocean, no destination
//!   to pick (there is only ever one), no fan-out ever. Mirrors
//!   `routes::gmail_everything_mode::set_everything_mode`'s "owner
//!   configures their own mailbox, nobody else's" shape, narrowed to a
//!   single POST (there is no `{"enabled": bool}` toggle — see
//!   [`opt_out_of_mailbox_space`] for the off switch).
//! * `DELETE /v1/connectors/google/chat/spaces/{space_id}/optin` —
//!   [`opt_out_of_mailbox_space`] (G4.5). Deactivates the CALLING member's
//!   own opt-in — never another member's, and never a container row (this
//!   route only ever touches `owner_user_id = user.user_id` rows).
//!
//! `{space_id}` in every route's path is the BARE Chat resource suffix
//! (e.g. `AAAAAAAAAAA`, not `spaces/AAAAAAAAAAA`) — a Chat space's full
//! resource name contains its own `/`, which axum's path-segment matching
//! cannot carry as one param; [`full_space_id`] reconstructs the real
//! `"spaces/{id}"` string every DB read/write and Chat API call actually
//! uses.
//!
//! # G4.6 CLOSES member-scoping before lifting the org gate (security review
//! finding L9)
//!
//! Through G4 PR 5, [`reject_org_google_chat_management`] refused
//! `Unrestricted` on the ONLY plane these routes served (Personal — Slack's
//! own precedent for its equivalent gate), so this finding was dormant: as
//! written back then, [`google_chat::pick_space_destination`]'s only check —
//! that `graph_id` belongs to `user.account_id` — is correct on Personal (one
//! account, one implicit owner) but would let ANY org member route ANY
//! watched space into ANY graph in the account the moment a member-scoped
//! caller exists.
//!
//! **This is SEATING-BEARING (gsuite Train G4 PR 4, security review), so it
//! is closed BEFORE this PR lifts the gate below, not alongside it.** A
//! destination pick ARMS a backfill and causes the sweep's roster reconcile
//! to auto-seat every matched roster member onto the destination team ocean
//! with `history_from = NULL` — i.e. unbounded history of that ocean,
//! including everything predating the space. So an unscoped pick is not "an
//! authorisation an admin would have made anyway"; it is a
//! privilege-escalation primitive the moment a second member exists.
//! [`reject_unauthorized_org_chat_destination_pick`] is the fix: the SAME
//! per-`graph.kind` authorization `connectors::slack_channel_actions::
//! set_channel_destination` already enforces for Slack's equivalent REST
//! surface, reused (not re-implemented) via that module's
//! `fetch_graph_for_destination`/`fetch_team_id_for_graph` and
//! `routes::slack_interactivity::check_team_ocean_admin`. It runs in
//! [`set_space_destination`] BEFORE [`google_chat::pick_space_destination`]
//! is ever called, so the underlying account-ownership check stays exactly
//! as it was — this is an ADDITIVE gate in front of it, not a replacement.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::{
    connectors::{
        google::GoogleConnector,
        google_chat::{self, MailboxOptInError, PickError, PickedDestination},
        TokenOwner,
    },
    middleware::auth::UserContext,
    routes::{
        connectors::load_creds_bg,
        oauth::store_token_bundle,
        slack::{resolve_primary_graph, AccountPlane, IngestCaller},
    },
    tenancy::extractor::Tenant,
    AppState,
};

/// Same refusal shape as `routes::slack::reject_org_slack_management` —
/// through gsuite Train G4 PR 5 this refused every org `TenancyDecision`
/// unconditionally, flag or no flag. **gsuite Train G4 PR 6 ("G4.6",
/// founder decision, mirroring the gsuite Train G2 PR 6 Gmail org-plane lift
/// — `docs/connectors/google-workspace.md` §2) lifts it behind
/// `ORG_GOOGLE_CHAT_ENABLED`**: gated on
/// [`crate::routes::oauth::org_google_chat_enabled_for`], the SAME registry
/// flag (`org_connector_flags`, provider key `google_chat`) that already
/// gates `routes::oauth::authorize`'s Chat surface and (as of this PR)
/// `callback`/`mint_state`/`list_connections`'s own Chat arms — one flag, one
/// pilot toggle, no separate "may connect" vs "may manage watched spaces"
/// switch. `routes::google_chat`'s functions stay plane-explicit (design doc
/// §3.8) regardless — this gate decides WHETHER the org branch runs, not
/// whether it exists.
async fn reject_org_google_chat_management(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if crate::routes::oauth::org_google_chat_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "google chat management route refused under org context (ORG_GOOGLE_CHAT_ENABLED/\
         org_connector_flags resolved off for this org)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "Google Chat is available in your personal workspace only; switch out \
                          of the organization workspace to manage it"
            })),
        )
            .into_response(),
    )
}

/// G4.6's fix for the security-review finding this module's own doc comment
/// names (L9, "UPDATED... this deferral is now SEATING-BEARING, and must be
/// closed BEFORE the org gate lifts — not alongside it"). Once
/// [`reject_org_google_chat_management`] admits an org account (behind
/// `ORG_GOOGLE_CHAT_ENABLED`), [`google_chat::pick_space_destination`]'s only
/// remaining check — that `graph_id` belongs to the CALLING ACCOUNT — stops
/// being sufficient the moment a second member exists: a destination pick
/// ARMS a backfill and auto-seats every matched roster member onto the
/// destination graph with `history_from = NULL` (unbounded history,
/// including everything predating the space), so an unauthorised pick is a
/// privilege-escalation primitive, not just an inconvenience.
///
/// Mirrors `connectors::slack_channel_actions::set_channel_destination`'s own
/// per-`graph.kind` authorization exactly — reusing its
/// `fetch_graph_for_destination`/`fetch_team_id_for_graph` and
/// `routes::slack_interactivity::check_team_ocean_admin` directly rather than
/// re-implementing them, so the two REST "pick a destination" surfaces
/// (Slack's, Chat's) can never drift on what counts as authorized:
///
/// * `kind == "org"` — refused; the org ocean is never a valid connector
///   destination, on either provider.
/// * `kind == "team"` — the caller must administer that team
///   ([`crate::routes::slack_interactivity::check_team_ocean_admin`]).
/// * `kind == "member"` — the caller may only route into their OWN private
///   ocean, never a different member's.
/// * Anything else (a `personal`-kind graph, or a `member`-kind graph that IS
///   the caller's own) — no further check needed;
///   [`google_chat::pick_space_destination`]'s own ownership check (still
///   real, still runs) is enough, same as it already is on the Personal
///   plane.
///
/// A no-op on the Personal plane (`tenant.org_context()` is `None` — byte-
/// identical to before this PR) and for `PickedDestination::FanOut` (fanning
/// seats out through the identity matcher never routes into a NAMED
/// member's ocean, so there is nothing here to authorize beyond the
/// existing `FanoutRequiresOrgPlane` guard). Fails CLOSED on a lookup error:
/// an authorization check that cannot complete must never read as
/// "authorized".
async fn reject_unauthorized_org_chat_destination_pick(
    tenant: &Tenant,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    destination: &PickedDestination,
) -> Option<Response> {
    let ctx = tenant.org_context()?;
    let graph_id = match destination {
        PickedDestination::Single(id) => *id,
        PickedDestination::FanOut => return None,
    };

    let graph = match crate::connectors::slack_channel_actions::fetch_graph_for_destination(
        pool, account_id, graph_id,
    )
    .await
    {
        Ok(Some(g)) => g,
        // Not found (or archived, or cross-account) — `pick_space_destination`'s
        // own ownership check refuses this on its own moments later; nothing
        // to authorize here.
        Ok(None) => return None,
        Err(e) => {
            tracing::error!(error = %e, org_id = %ctx.org_id, %account_id, %graph_id, "google chat: org destination-pick authorization lookup failed; refusing (fail closed)");
            return Some(chat_destination_forbidden(
                "could not verify authorization for this ocean; try again",
            ));
        }
    };

    match graph.kind.as_str() {
        "org" => Some(chat_destination_forbidden(
            "the organization ocean is not a valid Chat destination",
        )),
        "team" => {
            let team_id = match crate::connectors::slack_channel_actions::fetch_team_id_for_graph(
                pool, account_id, graph_id,
            )
            .await
            {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return Some(chat_destination_forbidden(
                        "that team ocean could not be found",
                    ))
                }
                Err(e) => {
                    tracing::error!(error = %e, org_id = %ctx.org_id, %account_id, %graph_id, "google chat: org destination-pick team lookup failed; refusing (fail closed)");
                    return Some(chat_destination_forbidden(
                        "could not verify authorization for this ocean; try again",
                    ));
                }
            };
            match crate::routes::slack_interactivity::check_team_ocean_admin(
                pool,
                account_id,
                Some(user_id),
                team_id,
            )
            .await
            {
                Ok(crate::routes::slack_interactivity::TeamOceanCheck::Admin { .. }) => None,
                Ok(crate::routes::slack_interactivity::TeamOceanCheck::NotAdmin { .. }) => {
                    tracing::info!(org_id = %ctx.org_id, %account_id, %user_id, %team_id, "google chat: org destination pick refused — caller does not administer this team");
                    Some(chat_destination_forbidden(
                        "routing a Chat space into a team ocean requires administering that team",
                    ))
                }
                Ok(crate::routes::slack_interactivity::TeamOceanCheck::NotFound) => Some(
                    chat_destination_forbidden("that team ocean could not be found"),
                ),
                Err(e) => {
                    tracing::error!(error = %e, org_id = %ctx.org_id, %account_id, %team_id, "google chat: org destination-pick admin check failed; refusing (fail closed)");
                    Some(chat_destination_forbidden(
                        "could not verify authorization for this ocean; try again",
                    ))
                }
            }
        }
        "member" if graph.owner_user_id != Some(user_id) => {
            tracing::info!(org_id = %ctx.org_id, %account_id, %user_id, %graph_id, "google chat: org destination pick refused — not the caller's own private ocean");
            Some(chat_destination_forbidden(
                "you may only route a Chat space into your own private ocean",
            ))
        }
        _ => None,
    }
}

fn chat_destination_forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}

fn full_space_id(bare: &str) -> String {
    if bare.starts_with("spaces/") {
        bare.to_string()
    } else {
        format!("spaces/{bare}")
    }
}

/// `GET /v1/connectors/google/chat/spaces`
pub async fn list_spaces(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_google_chat_management(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    // `oauth_tokens`/the account DEK are CONTROL-ONLY (gsuite Train G2 PR 6's
    // `ingest_selected` gives the exact reasoning, reused here verbatim now
    // that G4.6 admits an org caller): `pool` (== `tenant.pool`) is the
    // CONTENT pool — the legacy consumer pool on Personal, the org's own
    // TENANT database once `reject_org_google_chat_management` admits an
    // org caller — and `google_chat_watched_spaces` genuinely lives there.
    // Credentials never do. `state.control_db()` is the exact pool
    // `routes::oauth::callback` already writes `oauth_tokens` through.
    let Some(creds_pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };

    // The DEK plane comes off the request's own binding-gate decision, not a
    // pinned `consumer` — on Personal this resolves to `creds_pool` itself;
    // on Org it resolves to `ctx.pool`/`ctx.account_id` off the decision
    // (`DekPlane::from_decision`'s own doc), never `creds_pool` directly, so
    // an org account's DEK is read from ITS tenant plane (`coralswarm-org-
    // dek-tenant-plane` decision), not silently from control.
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), creds_pool);
    let Some(mut creds) = (match load_creds_bg(creds_pool, &dek_plane, user.account_id, "google_chat", TokenOwner::Member(user.user_id)).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, "failed to load google_chat creds");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }) else {
        // Not connected — nothing live to merge; still return the account's
        // local rows (a colleague may have added the app to a shared space
        // this member has never personally connected Chat for) so the UI
        // has something to show rather than an empty page.
        return spaces_response(&pool, user.account_id, user.user_id, &[]).await;
    };

    let google = GoogleConnector;
    if let Err(e) = google.refresh_if_needed(&state.http_client, &mut creds).await {
        tracing::warn!(error = %e, account_id = %user.account_id, "google_chat token refresh failed");
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "could not refresh your Google Chat connection"})),
        )
            .into_response();
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(creds_pool, &dek_plane, user.account_id, "google_chat", TokenOwner::Member(user.user_id), refreshed).await {
            tracing::warn!(status = ?e.status(), account_id = %user.account_id, "failed to persist refreshed google_chat token (non-fatal)");
        }
    }

    let key = format!("google_chat:{}", user.account_id);
    let live = crate::graph_rate_limit::scoped(key, async {
        google_chat::list_spaces(&state.http_client, &creds.access_token).await
    })
    .await;
    let live_spaces = match live {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, "chat spaces.list failed");
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "could not read your Google Chat spaces"})),
            )
                .into_response();
        }
    };
    reconcile_app_membership(&state, &pool, creds_pool, &dek_plane, &user, &creds.access_token, &live_spaces).await;
    spaces_response(&pool, user.account_id, user.user_id, &live_spaces).await
}

/// Self-heal the app-install gate from Google's own state. The watched-space
/// row is otherwise written ONLY by an `ADDED_TO_SPACE` push that also finds
/// the adder with a connected `google_chat` token at that instant; an add
/// that arrived before the member connected (or while the push path was
/// failing) was deferred and never retried, leaving a space whose Apps list
/// shows CoralSwarm permanently labelled "app not added". For each named
/// space without an active row, ask Chat whether the app is a `JOINED`
/// member (`members/app`, caller's own token) and, if so, create the same
/// unrouted row `ADDED_TO_SPACE` would have. Best effort: any failure leaves
/// the listing exactly as before.
async fn reconcile_app_membership(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    creds_pool: &sqlx::PgPool,
    dek_plane: &crate::crypto::DekPlane,
    user: &UserContext,
    access_token: &str,
    live: &[crate::connectors::chat_body::ChatSpace],
) {
    let watched = match google_chat::list_watched_spaces(pool, user.account_id).await {
        Ok(w) => w,
        Err(_) => return, // spaces_response surfaces the real error
    };
    let plane = google_chat::added_to_space_account_plane(dek_plane.is_org());
    let cursor = google_chat::app_probe_cursor(user.account_id);
    let batch = google_chat::spaces_needing_app_probe(live, &watched, cursor);
    google_chat::advance_app_probe_cursor(user.account_id, batch.len());
    for space in batch {
        let key = format!("google_chat:{}", user.account_id);
        let verdict = crate::graph_rate_limit::scoped(key, async {
            google_chat::get_app_membership(&state.http_client, access_token, &space.name).await
        })
        .await;
        match verdict {
            google_chat::AppMembership::Joined => {
                match google_chat::record_added_to_space(creds_pool, pool, plane, user.account_id, Some(user.user_id), space).await {
                    Ok(_) => tracing::info!(space = %space.name, account_id = %user.account_id, "google chat: app membership confirmed live; watched-space row created (unrouted)"),
                    Err(e) => tracing::warn!(space = %space.name, account_id = %user.account_id, error = %e, "google chat: live app-membership reconcile failed to record row"),
                }
            }
            google_chat::AppMembership::Absent => {}
            google_chat::AppMembership::Unknown => {
                tracing::warn!(space = %space.name, account_id = %user.account_id, "google chat: members/app probe inconclusive; leaving space as-is");
            }
        }
    }
}

/// Merges THREE views into one response, per `space_type` (design doc §9's
/// per-archetype picker):
///   * `SPACE` — the container archetype's app-install/destination-pick
///     state (`connected`/`active`/`destination`, unchanged since G4.3/G4.4).
///   * `GROUP_CHAT`/`DIRECT_MESSAGE` — G4.5's mailbox archetype: whether the
///     CALLING member (`owner_user_id`) has personally opted in
///     (`mailbox_optin`), never another member's opt-in state — this is the
///     destination-listing endpoint the mailbox opt-in route needs, folded
///     into the SAME picker rather than a second endpoint (mirrors how
///     `spaces.list` already returns every archetype in one call).
async fn spaces_response(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    live: &[crate::connectors::chat_body::ChatSpace],
) -> Response {
    let watched = match google_chat::list_watched_spaces(pool, account_id).await {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "failed to read google_chat_watched_spaces");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    let mailbox = match google_chat::list_watched_mailbox_spaces_for_owner(pool, account_id, owner_user_id).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, %owner_user_id, "failed to read this member's google_chat mailbox opt-ins");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    let mut spaces: Vec<serde_json::Value> = Vec::new();
    for space in live {
        if space.space_type == "SPACE" {
            let row = watched.get(&space.name);
            spaces.push(json!({
                "space_id": space.name.strip_prefix("spaces/").unwrap_or(&space.name),
                "display_name": space.display_name,
                "space_type": space.space_type,
                "history_state": space.history_state,
                "connected": row.is_some(),
                "active": row.map(|r| r.active).unwrap_or(false),
                "destination": destination_json(row),
            }));
        } else {
            // GROUP_CHAT / DIRECT_MESSAGE (G4.5): no app-install concept, no
            // destination to pick — only whether THIS member has opted in.
            let row = mailbox.get(&space.name);
            spaces.push(json!({
                "space_id": space.name.strip_prefix("spaces/").unwrap_or(&space.name),
                "display_name": space.display_name,
                "space_type": space.space_type,
                "history_state": space.history_state,
                "mailbox_optin": mailbox_optin_json(row),
            }));
        }
    }
    // Any watched CONTAINER row whose space no longer appears in the live
    // listing (e.g. this member was never in it, or lost access) still needs
    // to be visible — the app-install gate (D6) is a real state independent
    // of whether THIS caller can currently see the space via their own
    // token. No mailbox analogue: a mailbox opt-in is always this SAME
    // member's own live space (an opt-in the caller made through their own
    // `spaces.list`), so there is no "opted in but can no longer see it"
    // state to backfill the way there is for a colleague's container pick.
    for (space_id, row) in &watched {
        if live.iter().any(|s| &s.name == space_id) {
            continue;
        }
        spaces.push(json!({
            "space_id": space_id.strip_prefix("spaces/").unwrap_or(space_id.as_str()),
            "display_name": row.display_name,
            "space_type": row.space_type,
            "history_state": row.history_state,
            "connected": true,
            "active": row.active,
            "destination": destination_json(Some(row)),
        }));
    }

    (StatusCode::OK, Json(json!({"spaces": spaces}))).into_response()
}

fn mailbox_optin_json(row: Option<&google_chat::MailboxSpaceRow>) -> serde_json::Value {
    match row {
        None => json!({"opted_in": false}),
        Some(row) => json!({"opted_in": true, "active": row.active, "graph_id": row.graph_id}),
    }
}

fn destination_json(row: Option<&google_chat::WatchedSpaceRow>) -> serde_json::Value {
    let Some(row) = row else {
        return json!({"kind": "not_connected"});
    };
    match row.mode.as_deref() {
        None => json!({"kind": "unrouted"}),
        Some("single") => json!({"kind": "single", "graph_id": row.graph_id}),
        Some("fanout") => json!({"kind": "fanout"}),
        Some(other) => {
            tracing::warn!(mode = other, "google_chat_watched_spaces: unrecognized mode — should be unreachable under the table's own CHECK constraint");
            json!({"kind": "unrouted"})
        }
    }
}

// ---------------------------------------------------------------------------
// POST .../destination
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DestinationPickBody {
    Single { graph_id: Uuid },
    Fanout,
}

impl From<DestinationPickBody> for PickedDestination {
    fn from(body: DestinationPickBody) -> Self {
        match body {
            DestinationPickBody::Single { graph_id } => PickedDestination::Single(graph_id),
            DestinationPickBody::Fanout => PickedDestination::FanOut,
        }
    }
}

fn pick_error_response(e: PickError) -> Response {
    match e {
        PickError::NotConnected => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
        PickError::GraphNotOwned => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
        PickError::FanoutRequiresOrgPlane => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
        PickError::Db(msg) => {
            tracing::warn!(error = %msg, "pick_space_destination failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

/// `POST /v1/connectors/google/chat/spaces/{space_id}/destination`
///
/// Persists the pick ([`google_chat::pick_space_destination`]), then
/// computes the destination-prompt disclosure (contract §5.4) as a
/// best-effort PREVIEW — see `connectors::google_chat`'s module doc for why
/// this is deliberately not G4.4's exact seating count. Uses whichever
/// connected Chat token is available: the space's own designated reader
/// (set at `ADDED_TO_SPACE` time, if that member is still connected), else
/// the picking caller's own token, else the disclosure reports
/// `"available": false` rather than blocking the pick itself — I2's
/// authorisation act is the pick, not the count.
pub async fn set_space_destination(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(space_id): Path<String>,
    Json(body): Json<DestinationPickBody>,
) -> Response {
    if let Some(reject) = reject_org_google_chat_management(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    // Same CONTROL-vs-CONTENT pool split as `list_spaces` — see its own
    // comment. `graphs`/`teams`/`google_chat_watched_spaces` are tenant-tree
    // tables (`pool`); `oauth_tokens`/`account_members` are control-only
    // (`creds_pool`).
    let Some(creds_pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    let plane = if tenant.org_context().is_some() {
        AccountPlane::Org
    } else {
        AccountPlane::Personal
    };
    let space_id = full_space_id(&space_id);
    let destination: PickedDestination = body.into();

    // G4.6's fix for security review finding L9 — see
    // `reject_unauthorized_org_chat_destination_pick`'s own doc comment. A
    // no-op on the Personal plane and for `FanOut`; runs BEFORE
    // `pick_space_destination` so an unauthorized org pick never reaches the
    // write (or arms a backfill / auto-seat for a graph the caller had no
    // business routing into).
    if let Some(reject) = reject_unauthorized_org_chat_destination_pick(
        &tenant,
        &pool,
        user.account_id,
        user.user_id,
        &destination,
    )
    .await
    {
        return reject;
    }

    let outcome = match google_chat::pick_space_destination(
        creds_pool,
        &pool,
        plane,
        user.account_id,
        &space_id,
        destination,
    )
    .await
    {
        Ok(o) => o,
        Err(e) => return pick_error_response(e),
    };

    // gsuite Train G4 PR 4: a NEWLY routed space arms its backfill and
    // acquires a designated reader. Both are best-effort — neither may
    // undo an authorisation the human just gave (I2), and the sweep worker
    // re-elects a reader on its own next tick if this fails.
    //
    // **Arming the backfill is what makes §5.4's promise true.** `0198`
    // defaults `backfill_complete` to TRUE ("a row with no backfill
    // machinery reads as nothing-pending"), so without this a routed space
    // would only ever ingest messages posted AFTER the pick — auto-seated
    // members would be told they read the ocean back to its start and would
    // in fact see nothing older than the click. `arm_backfill_for_space`
    // refuses to re-arm a space that has already ingested anything, so a
    // re-pick after a G4-Q6 re-add never restarts a completed backfill.
    if outcome.newly_routed {
        match google_chat::arm_backfill_for_space(&pool, user.account_id, &space_id, None).await {
            Ok(armed) => tracing::info!(
                account_id = %user.account_id, space_id = %space_id, armed,
                "google chat: destination picked — backfill armed for the next sweep tick"
            ),
            Err(e) => tracing::warn!(
                error = %e, account_id = %user.account_id, space_id = %space_id,
                connector_health = true,
                "google chat: could not arm the backfill for a newly routed space — live \
                 messages will still ingest, but history before the pick will not until this \
                 is repaired"
            ),
        }
        if outcome.row.reader_user_id.is_none() {
            match google_chat::elect_space_reader(&pool, user.account_id, &[]).await {
                Ok(reader) => {
                    if let Err(e) = google_chat::set_space_reader(&pool, user.account_id, &space_id, reader).await {
                        tracing::warn!(error = %e, account_id = %user.account_id, space_id = %space_id, "google chat: could not persist the elected reader (the sweep re-elects next tick)");
                    }
                }
                Err(e) => tracing::warn!(error = %e, account_id = %user.account_id, space_id = %space_id, "google chat: reader election failed (the sweep re-elects next tick)"),
            }
        }
    }

    // Best-effort disclosure preview — never blocks the response above.
    //
    // Deliberately borrows `reader_user_id`'s credential when it differs
    // from the picking caller (L5, security review round 2): the
    // designated reader was set at `ADDED_TO_SPACE` time to whichever
    // member's own token was used to authoritatively read `spaces.get`
    // (`record_added_to_space`) — that member's token is what actually
    // reads this space's roster going forward (D8, design doc §3.5), so a
    // DIFFERENT member picking the destination still gets a real,
    // non-stubbed disclosure rather than "unavailable" just because they
    // personally haven't connected Chat. Both users are members of the
    // SAME account (`user.account_id` — never a cross-account borrow), and
    // the credential never leaves this process or reaches the caller; only
    // the resulting COUNTS are returned.
    //
    // Gated on CURRENT membership (security review round 2, same finding as
    // `workers::google_chat_sweep::reader_credentials`): the borrow is only
    // legitimate while the designated reader is still a member of this
    // account. An offboarded ex-member's `oauth_tokens` row survives
    // offboarding by design, so without this check their credential would
    // still be loaded and used to call Google on our behalf. Falling back to
    // the caller is always safe — they are authenticated on this very
    // account, so they are a member by construction. An `Err` (we cannot
    // tell) falls back too: fail closed, never borrow on an unknown.
    //
    // The two gates below answer DIFFERENT questions and both are required
    // (this is the #743/#775 merge point). `is_current_account_member`
    // decides WHETHER this credential may be used at all; `DekPlane` decides
    // WHICH DATABASE its key is read from. Membership is settled FIRST, so
    // an ex-member's row is never even a candidate for a decrypt — the plane
    // resolution below runs on a reader we have already established is
    // entitled to be borrowed.
    // `is_current_account_member` reads `account_members` — CONTROL-only,
    // same as `list_spaces`' own credential pool, never `pool` (content).
    let reader = match outcome.row.reader_user_id {
        Some(uid)
            if google_chat::is_current_account_member(creds_pool, user.account_id, uid)
                .await
                .unwrap_or(false) =>
        {
            uid
        }
        _ => user.user_id,
    };
    // Same plane rule as `list_spaces` — the request's own decision, not a
    // pinned consumer plane.
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), creds_pool);
    let disclosure_creds = load_creds_bg(creds_pool, &dek_plane, user.account_id, "google_chat", TokenOwner::Member(reader))
        .await
        .ok()
        .flatten();
    let seating = match disclosure_creds {
        Some(creds) => {
            let key = format!("google_chat:{}", user.account_id);
            // `seating_disclosure`'s `control_pool` resolves identities
            // against `oauth_tokens`/(indirectly) `account_members` —
            // CONTROL-only, never the content `pool`.
            let control_pool = creds_pool.clone();
            let http = state.http_client.clone();
            let space_id_for_disclosure = space_id.clone();
            let account_id = user.account_id;
            crate::graph_rate_limit::scoped(key, async move {
                google_chat::seating_disclosure(&http, &control_pool, account_id, &creds.access_token, &space_id_for_disclosure).await
            })
            .await
        }
        None => google_chat::SeatingDisclosure::default(),
    };

    (
        StatusCode::OK,
        Json(json!({
            "newly_routed": outcome.newly_routed,
            "destination": destination_json(Some(&outcome.row)),
            "history_off": outcome.row.history_state == "HISTORY_OFF",
            "seating": {
                "available": seating.available,
                "total_humans": seating.total_humans,
                "matched": seating.matched,
                "unmapped": seating.unmapped,
                "group_memberships": seating.group_memberships,
            },
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// G4.5 — the MAILBOX archetype opt-in (`POST`/`DELETE .../optin`)
// ---------------------------------------------------------------------------

fn mailbox_optin_error_response(e: MailboxOptInError) -> Response {
    match e {
        MailboxOptInError::WrongArchetype => (StatusCode::BAD_REQUEST, Json(json!({"error": e.to_string()}))).into_response(),
        MailboxOptInError::Db(msg) => {
            tracing::warn!(error = %msg, "record_mailbox_optin failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

/// `POST /v1/connectors/google/chat/spaces/{space_id}/optin`
///
/// Opts the CALLING member into a `GROUP_CHAT`/`DIRECT_MESSAGE` space —
/// unlike [`set_space_destination`], there is no destination to choose:
/// content lands exclusively in the caller's own primary private ocean
/// (`resolve_primary_graph`, `IngestCaller::Attributable` — the SAME helper
/// `routes::gmail_everything_mode::set_everything_mode` uses for the
/// identical "owner's own mailbox, owner's own private ocean" carve-out),
/// and there is never a fan-out option to offer.
///
/// `spaces.get` is re-read with the CALLER's own token before writing
/// anything (never trusting a caller-supplied archetype) — same defensive
/// posture `routes::google_push`'s `ADDED_TO_SPACE` arm takes before calling
/// [`google_chat::record_added_to_space`]. A `SPACE` result is refused with
/// [`MailboxOptInError::WrongArchetype`] before any write is attempted.
///
/// On success, reuses the SAME backfill-arming call
/// [`set_space_destination`] does for the container archetype
/// ([`google_chat::arm_backfill_for_space`], `owner_user_id =
/// Some(user.user_id)`) — best-effort: a failure here does not fail the
/// opt-in itself (live messages still ingest; history predating the opt-in
/// simply waits for a repaired arm, exactly as the container path's own
/// comment documents).
pub async fn opt_in_to_mailbox_space(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(space_id): Path<String>,
) -> Response {
    if let Some(reject) = reject_org_google_chat_management(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    // CONTROL-plane read: `account_members` exists only on the control DB.
    let Some(control_pool) = state.control_db() else { return StatusCode::SERVICE_UNAVAILABLE.into_response(); };
    if !matches!(google_chat::is_current_account_member(control_pool, user.account_id, user.user_id).await, Ok(true)) { return StatusCode::FORBIDDEN.into_response(); }
    let space_id = full_space_id(&space_id);

    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), &pool);
    let Some(mut creds) = (match load_creds_bg(control_pool, &dek_plane, user.account_id, "google_chat", TokenOwner::Member(user.user_id)).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, "failed to load google_chat creds");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "connect Google Chat before opting a direct message or group chat in"})),
        )
            .into_response();
    };

    let google = GoogleConnector;
    if let Err(e) = google.refresh_if_needed(&state.http_client, &mut creds).await {
        tracing::warn!(error = %e, account_id = %user.account_id, "google_chat token refresh failed");
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "could not refresh your Google Chat connection"})),
        )
            .into_response();
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(control_pool, &dek_plane, user.account_id, "google_chat", TokenOwner::Member(user.user_id), refreshed).await {
            tracing::warn!(status = ?e.status(), account_id = %user.account_id, "failed to persist refreshed google_chat token (non-fatal)");
        }
    }

    let key = format!("google_chat:{}", user.account_id);
    let space = match crate::graph_rate_limit::scoped(key, google_chat::get_space(&state.http_client, &creds.access_token, &space_id)).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, space_id = %space_id, "chat spaces.get failed for a mailbox opt-in");
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "could not read this space from Google Chat"})),
            )
                .into_response();
        }
    };

    let graph_id = match resolve_primary_graph(&pool, user.account_id, if tenant.org_context().is_some() { AccountPlane::Org } else { AccountPlane::Personal }, IngestCaller::Attributable(user.user_id)).await {
        Ok(g) => g,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, "could not resolve the caller's private ocean for a chat mailbox opt-in");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "your private workspace is not ready yet — try again shortly"})),
            )
                .into_response();
        }
    };

    let row = match google_chat::record_mailbox_optin(&pool, user.account_id, user.user_id, &space, graph_id).await {
        Ok(r) => r,
        Err(e) => return mailbox_optin_error_response(e),
    };

    // Same reuse as `set_space_destination`'s own backfill-arming call —
    // best-effort, never blocks the opt-in response.
    match google_chat::arm_backfill_for_space(&pool, user.account_id, &space_id, Some(user.user_id)).await {
        Ok(armed) => tracing::info!(
            account_id = %user.account_id, space_id = %space_id, armed,
            "google chat: mailbox opt-in recorded — backfill armed for the next sweep tick"
        ),
        Err(e) => tracing::warn!(
            error = %e, account_id = %user.account_id, space_id = %space_id,
            connector_health = true,
            "google chat: could not arm the backfill for a newly opted-in mailbox space — live \
             messages will still ingest, but history before the opt-in will not until this is \
             repaired"
        ),
    }

    (
        StatusCode::OK,
        Json(json!({
            "space_id": row.space_id.strip_prefix("spaces/").unwrap_or(&row.space_id),
            "display_name": row.display_name,
            "space_type": row.space_type,
            "graph_id": row.graph_id,
            "active": row.active,
            "history_off": row.history_state == "HISTORY_OFF",
        })),
    )
        .into_response()
}

/// `DELETE /v1/connectors/google/chat/spaces/{space_id}/optin`
///
/// Opts the CALLING member back OUT — `owner_user_id = user.user_id` always,
/// so this route can never touch another member's opt-in or a container row.
pub async fn opt_out_of_mailbox_space(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(space_id): Path<String>,
) -> Response {
    if let Some(reject) = reject_org_google_chat_management(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    // CONTROL-plane read: `account_members` exists only on the control DB.
    let Some(control_pool) = state.control_db() else { return StatusCode::SERVICE_UNAVAILABLE.into_response(); };
    if !matches!(google_chat::is_current_account_member(control_pool, user.account_id, user.user_id).await, Ok(true)) { return StatusCode::FORBIDDEN.into_response(); }
    let space_id = full_space_id(&space_id);

    match google_chat::opt_out_of_mailbox_space(&pool, user.account_id, user.user_id, &space_id).await {
        Ok(true) => (StatusCode::OK, Json(json!({"active": false}))).into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "you have not opted this space in"})),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user.account_id, space_id = %space_id, "opt_out_of_mailbox_space failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection as _;

    // ── Pure helpers ─────────────────────────────────────────────────────

    #[test]
    fn full_space_id_adds_prefix_when_bare_leaves_alone_when_already_full() {
        assert_eq!(full_space_id("AAAAAAAAAAA"), "spaces/AAAAAAAAAAA");
        assert_eq!(full_space_id("spaces/AAAAAAAAAAA"), "spaces/AAAAAAAAAAA");
    }

    fn fixture_row(mode: Option<&str>, graph_id: Option<Uuid>, active: bool) -> google_chat::WatchedSpaceRow {
        google_chat::WatchedSpaceRow {
            id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            space_id: "spaces/AAA".to_string(),
            space_type: "SPACE".to_string(),
            mode: mode.map(str::to_string),
            graph_id,
            reader_user_id: None,
            display_name: "eng-platform".to_string(),
            history_state: "HISTORY_ON".to_string(),
            active,
        }
    }

    #[test]
    fn destination_json_none_row_is_not_connected() {
        assert_eq!(destination_json(None), json!({"kind": "not_connected"}));
    }

    #[test]
    fn destination_json_unrouted_when_mode_is_null() {
        let row = fixture_row(None, None, true);
        assert_eq!(destination_json(Some(&row)), json!({"kind": "unrouted"}));
    }

    #[test]
    fn destination_json_single_carries_graph_id() {
        let graph_id = Uuid::new_v4();
        let row = fixture_row(Some("single"), Some(graph_id), true);
        assert_eq!(destination_json(Some(&row)), json!({"kind": "single", "graph_id": graph_id}));
    }

    #[test]
    fn destination_json_fanout() {
        let row = fixture_row(Some("fanout"), None, true);
        assert_eq!(destination_json(Some(&row)), json!({"kind": "fanout"}));
    }

    #[test]
    fn destination_pick_body_deserializes_single_and_fanout() {
        let graph_id = Uuid::new_v4();
        let single: DestinationPickBody = serde_json::from_value(json!({"kind": "single", "graph_id": graph_id})).unwrap();
        assert!(matches!(PickedDestination::from(single), PickedDestination::Single(g) if g == graph_id));

        let fanout: DestinationPickBody = serde_json::from_value(json!({"kind": "fanout"})).unwrap();
        assert!(matches!(PickedDestination::from(fanout), PickedDestination::FanOut));

        let bad: Result<DestinationPickBody, _> = serde_json::from_value(json!({"kind": "bogus"}));
        assert!(bad.is_err());
    }

    // ── Org-gate smoke test (security review finding L9 / gsuite Train G4
    // PR 6, "G4.6") ──────────────────────────────────────────────────────────
    // Through G4 PR 5 this gate refused every org `TenancyDecision`
    // unconditionally. G4.6 lifts it behind `ORG_GOOGLE_CHAT_ENABLED` — see
    // `reject_org_google_chat_management`'s own doc comment. These pin the
    // gate's flag-OFF and Personal-plane behaviour with no control pool
    // configured (so `connector_flags::resolve` falls back to the raw env
    // default, which is `false`/unset in a bare unit-test process); the
    // flag-ON admission path is pg-tested below against a real per-org
    // `org_connector_flags` row, never by mutating process-global env vars
    // here (this file's tests run alongside `routes::oauth`'s in the same
    // `cargo test --lib "google_chat"` process, and that file already owns
    // `ORG_GOOGLE_CHAT_ENABLED`'s only env-mutating lock).

    fn org_tenant() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_google_chat_gate_test".to_string(),
                    account_id: Uuid::new_v4(),
                    user_id: Uuid::new_v4(),
                    pool: sqlx::postgres::PgPoolOptions::new()
                        .connect_lazy("postgres://localhost/unused")
                        .expect("lazy pool construction never connects"),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn personal_tenant() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    // `#[tokio::test]`, not `#[test]`: `org_tenant()`'s `connect_lazy` needs
    // a Tokio runtime context to construct the (never-connected) lazy pool,
    // and the gate itself is now `async` (control-plane flag resolution).
    #[tokio::test]
    async fn reject_org_google_chat_management_refuses_org_when_flag_off() {
        let reject =
            reject_org_google_chat_management(&crate::test_app_state(), &org_tenant()).await;
        assert!(
            reject.is_some(),
            "an org TenancyDecision must be refused while ORG_GOOGLE_CHAT_ENABLED/\
             org_connector_flags resolves off for this org"
        );
    }

    #[tokio::test]
    async fn reject_org_google_chat_management_is_a_noop_on_the_personal_plane() {
        assert!(
            reject_org_google_chat_management(&crate::test_app_state(), &personal_tenant())
                .await
                .is_none(),
            "Personal-plane callers must never be refused by this gate"
        );
    }

    // ── `reject_unauthorized_org_chat_destination_pick` — pure structural
    // cases that need no DB (a graph lookup failure fails closed; `FanOut`
    // and Personal are no-ops) ──────────────────────────────────────────────

    #[tokio::test]
    async fn destination_pick_authz_is_a_noop_on_the_personal_plane() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool construction never connects");
        let reject = reject_unauthorized_org_chat_destination_pick(
            &personal_tenant(),
            &pool,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &PickedDestination::Single(Uuid::new_v4()),
        )
        .await;
        assert!(reject.is_none(), "Personal-plane picks are never gated by this check");
    }

    #[tokio::test]
    async fn destination_pick_authz_is_a_noop_for_fanout_under_org() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool construction never connects");
        let reject = reject_unauthorized_org_chat_destination_pick(
            &org_tenant(),
            &pool,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &PickedDestination::FanOut,
        )
        .await;
        assert!(
            reject.is_none(),
            "FanOut names no specific graph to authorize against — nothing to check here"
        );
    }

    // ── Real Postgres, org-plane, gate LIFTED (gsuite Train G4 PR 6, "G4.6") ──
    //
    // Two genuinely SEPARATE databases — control (`accounts`/`account_members`/
    // `oauth_tokens`/`org_connector_flags`) and tenant (`graphs`/`teams`/
    // `team_members`/`google_chat_watched_spaces`) — because this is the
    // FIRST PR where an org account's Chat management routes read/write a
    // genuinely different database than its `oauth_tokens` row (see
    // `connectors::google_chat`'s own `space_cursor_round_trip_and_backfill_
    // arming_pg` comment: "on the personal plane the control database is the
    // content database" — that collapse is exactly what this test does NOT
    // rely on). Each pool connects as its plane's real least-privilege role
    // (`coralswarm_app` for control, `coralswarm_tenant_app` for tenant) —
    // never the DB owner — so a passing assertion here is evidence the RLS
    // policies on both planes actually let this flow through, not that
    // ownership bypassed them.
    //
    //   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
    //     cargo test -p coralswarm-backend --lib routes::google_chat::tests \
    //     -- --test-threads=1

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> sqlx::PgConnection {
        use sqlx::{Connection, Executor};
        let mut conn = sqlx::PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn drop_db(base: &str, db: &str) {
        use sqlx::{Connection, Executor};
        if let Ok(mut admin_conn) = sqlx::PgConnection::connect(base).await {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// Set only on this test's ephemeral scratch cluster. `coralswarm_app` is
    /// created with no password by migration `0009`; not a real credential.
    /// Duplicated per test module rather than shared — same convention
    /// `connectors::google_chat`'s own `app_role_pool` and `workers::
    /// gmail_everything_mode`'s `tenant_app_pool` both already document for
    /// this exact "no crate-level cross-plane role helper exists" reason.
    const CONTROL_APP_ROLE_TEST_PASSWORD: &str = "g46_chat_org_gate_control_pw";
    const TENANT_APP_ROLE_TEST_PASSWORD: &str = "g46_chat_org_gate_tenant_pw";

    async fn set_role_password(owner: &sqlx::PgPool, role: &str, password: &str) {
        let stmt = format!("ALTER ROLE {role} WITH PASSWORD '{password}'");
        let mut last: Option<sqlx::Error> = None;
        for attempt in 0..10u32 {
            match sqlx::query(&stmt).execute(owner).await {
                Ok(_) => return,
                Err(e) => {
                    last = Some(e);
                    let backoff = 50 * u64::from(attempt + 1);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                }
            }
        }
        panic!("set test-only password on {role} after 10 attempts: {last:?}");
    }

    /// A pool connected AS `coralswarm_app` — the real, least-privilege
    /// CONTROL-plane role (never the DB owner).
    async fn control_app_pool(owner: &sqlx::PgPool, db_url: &str) -> sqlx::PgPool {
        set_role_password(owner, "coralswarm_app", CONTROL_APP_ROLE_TEST_PASSWORD).await;
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(db_url)
            .expect("parse control scratch db url")
            .username("coralswarm_app")
            .password(CONTROL_APP_ROLE_TEST_PASSWORD);
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .expect("connect AS coralswarm_app")
    }

    /// A pool connected AS `coralswarm_tenant_app` — the real, NOBYPASSRLS
    /// TENANT-plane role (never the DB owner).
    async fn tenant_app_pool(owner: &sqlx::PgPool, db_url: &str) -> sqlx::PgPool {
        set_role_password(owner, "coralswarm_tenant_app", TENANT_APP_ROLE_TEST_PASSWORD).await;
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(db_url)
            .expect("parse tenant scratch db url")
            .username("coralswarm_tenant_app")
            .password(TENANT_APP_ROLE_TEST_PASSWORD);
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .expect("connect AS coralswarm_tenant_app")
    }

    fn org_tenant_with_pool(
        org_id: &str,
        account_id: Uuid,
        user_id: Uuid,
        pool: sqlx::PgPool,
    ) -> Tenant {
        Tenant {
            store: None,
            pool: Some(pool.clone()),
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: org_id.to_string(),
                    account_id,
                    user_id,
                    pool,
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn user_ctx(account_id: Uuid, user_id: Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "test-device".to_string(),
        }
    }

    /// The org-plane gate lift end to end: `ORG_GOOGLE_CHAT_ENABLED` resolved
    /// from a real per-org `org_connector_flags` row (never a mutated process
    /// env var — see the unit tests above for why), an org member who does
    /// NOT administer the destination team refused by
    /// [`reject_unauthorized_org_chat_destination_pick`] (the L9 fix), and
    /// the team's actual admin succeeding — proving [`set_space_destination`]
    /// genuinely reaches its write against the org's own TENANT database
    /// under `coralswarm_tenant_app`, not the control database G4.6 replaces
    /// as the content plane.
    #[tokio::test]
    async fn org_mailbox_optin_uses_own_tenant_ocean_and_control_credentials_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_mailbox_optin_uses_own_tenant_ocean_and_control_credentials_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_mailbox_org_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_mailbox_org_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);

        let control_owner: sqlx::PgPool = {
            let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
            let conn = recreate(&base, control_db).await;
            conn.close().await.ok();
            sqlx::postgres::PgPoolOptions::new()
                .connect(&with_db(&base, control_db))
                .await
                .unwrap()
        };
        sqlx::migrate!("./migrations_control").run(&control_owner).await.unwrap();
        let control_app = control_app_pool(&control_owner, &with_db(&base, control_db)).await;

        let tenant_owner: sqlx::PgPool = {
            let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
            let conn = recreate(&base, tenant_db).await;
            conn.close().await.ok();
            sqlx::postgres::PgPoolOptions::new()
                .connect(&with_db(&base, tenant_db))
                .await
                .unwrap()
        };
        sqlx::migrate!("./migrations_tenant").run(&tenant_owner).await.unwrap();
        let org_id = format!("org_{}", Uuid::new_v4());
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(&org_id)
            .execute(&tenant_owner)
            .await
            .unwrap();
        let tenant_app = tenant_app_pool(&tenant_owner, &with_db(&base, tenant_db)).await;

        // ── Control-plane fixture: a real org account, two members (an
        // admin and a plain member), and the per-org flag ON. ─────────────
        let alice: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("alice-{}@test.local", Uuid::new_v4()))
        .fetch_one(&control_owner)
        .await
        .unwrap();
        let bob: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("bob-{}@test.local", Uuid::new_v4()))
        .fetch_one(&control_owner)
        .await
        .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(alice)
        .bind(&org_id)
        .fetch_one(&control_owner)
        .await
        .unwrap();
        // PR F (seat lifecycle): every `account_members` writer must pass an
        // explicit `seat_tier_id` — same `resolve_standard_seat_tier_id`
        // query `clerk_provision.rs` uses for a plain org join.
        let standard_tier_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(&control_owner)
        .await
        .unwrap();
        for (uid, role) in [(alice, "owner"), (bob, "member")] {
            sqlx::query(
                "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(account_id)
            .bind(uid)
            .bind(role)
            .bind(standard_tier_id)
            .execute(&control_owner)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO org_connector_flags (clerk_org_id, provider, enabled) \
             VALUES ($1, 'google_chat', true)",
        )
        .bind(&org_id)
        .execute(&control_owner)
        .await
        .unwrap();


        std::env::set_var("MASTER_KEK_HEX","cc".repeat(32));
        let wrapped = coralswarm_core::kek::wrap_dek(&coralswarm_core::kek::load_kek().unwrap(), &coralswarm_core::kek::generate_dek()).unwrap();
        sqlx::query("INSERT INTO account_keys(account_id,dek_wrapped) VALUES($1,$2)").bind(account_id).bind(wrapped).execute(&tenant_owner).await.unwrap();
        let graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,is_primary,owner_user_id) VALUES($1,'private','member',true,$2) RETURNING id").bind(account_id).bind(alice).fetch_one(&tenant_owner).await.unwrap();
        let alice_tenant=org_tenant_with_pool(&org_id,account_id,alice,tenant_app.clone());
        let plane=crate::crypto::DekPlane::from_decision(alice_tenant.decision.as_ref(),&tenant_app);
        let token=crate::connectors::TokenBundle { access_token:"alice-only".into(), refresh_token:None,user_token:None,scope:"chat.spaces.readonly".into(),expires_at:Some(chrono::Utc::now()+chrono::Duration::hours(1)),provider_workspace_id:"alice@example.com".into(),provider_workspace_name:None,provider_user_id:"sub-alice".into() };
        store_token_bundle(&control_app,&plane,account_id,"google_chat",TokenOwner::Member(alice),&token).await.unwrap();
        async fn space(headers:axum::http::HeaderMap)->Json<serde_json::Value> {
            assert_eq!(headers.get("authorization").unwrap(),"Bearer alice-only");
            Json(json!({"name":"spaces/DM","spaceType":"DIRECT_MESSAGE","historyState":"HISTORY_ON"}))
        }
        let mock=axum::Router::new().route("/spaces/DM",axum::routing::get(space));
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener,mock).await.unwrap(); });
        let state=Arc::new(crate::test_app_state_owned_with_db(control_app.clone(),None));
        google_chat::TEST_CHAT_API_BASE.scope(url,async {
            let reply=opt_in_to_mailbox_space(State(state.clone()),alice_tenant,Extension(user_ctx(account_id,alice,"owner")),Path("DM".into())).await;
            assert_eq!(reply.status(),StatusCode::OK);
        }).await;
        let rows:Vec<(Uuid,Uuid)>=sqlx::query_as("SELECT owner_user_id,graph_id FROM google_chat_watched_spaces WHERE account_id=$1").bind(account_id).fetch_all(&tenant_owner).await.unwrap();
        assert_eq!(rows,vec![(alice,graph)]);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM google_chat_watched_spaces").fetch_one(&control_owner).await.unwrap(),0);
        let bob_reply=opt_out_of_mailbox_space(State(state.clone()),org_tenant_with_pool(&org_id,account_id,bob,tenant_app.clone()),Extension(user_ctx(account_id,bob,"member")),Path("DM".into())).await;
        assert_eq!(bob_reply.status(),StatusCode::NOT_FOUND);
        let alice_reply=opt_out_of_mailbox_space(State(state.clone()),org_tenant_with_pool(&org_id,account_id,alice,tenant_app.clone()),Extension(user_ctx(account_id,alice,"owner")),Path("DM".into())).await;
        assert_eq!(alice_reply.status(),StatusCode::OK);
        crate::connector_flags::set_override(&control_owner,&state.connector_flag_cache,&org_id,"google_chat",false).await.unwrap();
        let disabled=opt_in_to_mailbox_space(State(state.clone()),org_tenant_with_pool(&org_id,account_id,alice,tenant_app.clone()),Extension(user_ctx(account_id,alice,"owner")),Path("DM".into())).await;
        assert_eq!(disabled.status(),StatusCode::FORBIDDEN);
        crate::connector_flags::set_override(&control_owner,&state.connector_flag_cache,&org_id,"google_chat",true).await.unwrap();
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(account_id).bind(bob).execute(&control_owner).await.unwrap();
        let departed=opt_out_of_mailbox_space(State(state.clone()),org_tenant_with_pool(&org_id,account_id,bob,tenant_app.clone()),Extension(user_ctx(account_id,bob,"member")),Path("DM".into())).await;
        assert_eq!(departed.status(),StatusCode::FORBIDDEN);
        drop(state);control_app.close().await;tenant_app.close().await;control_owner.close().await;tenant_owner.close().await;
        drop_db(&base,control_db).await;drop_db(&base,tenant_db).await;
    }

    #[tokio::test]
    async fn org_gate_lifted_admin_routes_non_admin_refused_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_gate_lifted_admin_routes_non_admin_refused_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_g46_gate_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_g46_gate_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);

        let control_owner: sqlx::PgPool = {
            let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
            let conn = recreate(&base, control_db).await;
            conn.close().await.ok();
            sqlx::postgres::PgPoolOptions::new()
                .connect(&with_db(&base, control_db))
                .await
                .unwrap()
        };
        sqlx::migrate!("./migrations_control").run(&control_owner).await.unwrap();
        let control_app = control_app_pool(&control_owner, &with_db(&base, control_db)).await;

        let tenant_owner: sqlx::PgPool = {
            let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
            let conn = recreate(&base, tenant_db).await;
            conn.close().await.ok();
            sqlx::postgres::PgPoolOptions::new()
                .connect(&with_db(&base, tenant_db))
                .await
                .unwrap()
        };
        sqlx::migrate!("./migrations_tenant").run(&tenant_owner).await.unwrap();
        let org_id = format!("org_{}", Uuid::new_v4());
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(&org_id)
            .execute(&tenant_owner)
            .await
            .unwrap();
        let tenant_app = tenant_app_pool(&tenant_owner, &with_db(&base, tenant_db)).await;

        // ── Control-plane fixture: a real org account, two members (an
        // admin and a plain member), and the per-org flag ON. ─────────────
        let alice: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("alice-{}@test.local", Uuid::new_v4()))
        .fetch_one(&control_owner)
        .await
        .unwrap();
        let bob: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("bob-{}@test.local", Uuid::new_v4()))
        .fetch_one(&control_owner)
        .await
        .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(alice)
        .bind(&org_id)
        .fetch_one(&control_owner)
        .await
        .unwrap();
        // PR F (seat lifecycle): every `account_members` writer must pass an
        // explicit `seat_tier_id` — same `resolve_standard_seat_tier_id`
        // query `clerk_provision.rs` uses for a plain org join.
        let standard_tier_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(&control_owner)
        .await
        .unwrap();
        for (uid, role) in [(alice, "owner"), (bob, "member")] {
            sqlx::query(
                "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(account_id)
            .bind(uid)
            .bind(role)
            .bind(standard_tier_id)
            .execute(&control_owner)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO org_connector_flags (clerk_org_id, provider, enabled) \
             VALUES ($1, 'google_chat', true)",
        )
        .bind(&org_id)
        .execute(&control_owner)
        .await
        .unwrap();

        // ── Tenant-plane fixture: a team ocean Alice administers, Bob does
        // NOT, and an already-watched (unrouted) space. ─────────────────────
        let team_graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Eng Ocean', 'team', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&tenant_owner)
        .await
        .unwrap();
        let team_id: Uuid = sqlx::query_scalar(
            "INSERT INTO teams (account_id, name, graph_id, created_by) \
             VALUES ($1, 'Eng', $2, $3) RETURNING id",
        )
        .bind(account_id)
        .bind(team_graph_id)
        .bind(alice)
        .fetch_one(&tenant_owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
            .bind(team_id)
            .bind(alice)
            .execute(&tenant_owner)
            .await
            .unwrap();
        // Bob deliberately gets NO `team_members` row — he is an account
        // member, but not an admin of THIS team.

        google_chat::record_added_to_space(
            &tenant_owner,
            &tenant_owner,
            AccountPlane::Org,
            account_id,
            Some(alice),
            &crate::connectors::chat_body::ChatSpace {
                name: "spaces/G46".to_string(),
                space_type: "SPACE".to_string(),
                display_name: "eng-platform".to_string(),
                space_threading_state: "THREADED_MESSAGES".to_string(),
                history_state: "HISTORY_ON".to_string(),
                single_user_bot_dm: false,
                membership_count: None,
            },
        )
        .await
        .unwrap()
        .expect("a SPACE archetype must create a watched row");

        let app_state = Arc::new(crate::test_app_state_owned_with_db(control_app.clone(), None));

        // ── Bob (non-admin) is refused BEFORE the routing write ─────────────
        let bob_tenant =
            org_tenant_with_pool(&org_id, account_id, bob, tenant_app.clone());
        let bob_resp = set_space_destination(
            State(app_state.clone()),
            bob_tenant,
            Extension(user_ctx(account_id, bob, "member")),
            Path("G46".to_string()),
            Json(DestinationPickBody::Single { graph_id: team_graph_id }),
        )
        .await;
        assert_eq!(
            bob_resp.status(),
            StatusCode::FORBIDDEN,
            "a non-admin org member must be refused routing a space into a team ocean he does \
             not administer — the L9 fix this PR closes before lifting the org gate"
        );
        let still_unrouted: Option<String> = sqlx::query_scalar(
            "SELECT mode FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = $2",
        )
        .bind(account_id)
        .bind("spaces/G46")
        .fetch_one(&tenant_owner)
        .await
        .unwrap();
        assert_eq!(
            still_unrouted, None,
            "Bob's refused pick must leave the space unrouted — no write, no backfill armed, no \
             seating triggered"
        );

        // ── Alice (the real team admin) succeeds — the gate lift's actual
        // point: the write lands on the org's own TENANT database, read back
        // here under `coralswarm_tenant_app`, not the control database. ────
        let alice_tenant =
            org_tenant_with_pool(&org_id, account_id, alice, tenant_app.clone());
        let alice_resp = set_space_destination(
            State(app_state.clone()),
            alice_tenant,
            Extension(user_ctx(account_id, alice, "owner")),
            Path("G46".to_string()),
            Json(DestinationPickBody::Single { graph_id: team_graph_id }),
        )
        .await;
        assert_eq!(
            alice_resp.status(),
            StatusCode::OK,
            "the team's real admin, with ORG_GOOGLE_CHAT_ENABLED on for this org, must be able \
             to route the space into the team ocean she administers"
        );
        let body = axum::body::to_bytes(alice_resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["newly_routed"], true);
        assert_eq!(json["destination"]["kind"], "single");
        assert_eq!(json["destination"]["graph_id"], team_graph_id.to_string());

        // Read back through `coralswarm_tenant_app` itself, inside the SAME
        // scoping helper every real write/read on this table uses
        // (`begin_tenant_system_scoped`) — an unscoped raw query under this
        // NOBYPASSRLS role would return zero rows regardless of what the
        // route just wrote, which would make this assertion pass for the
        // wrong reason (RLS hiding the row, not the row being absent).
        let mut verify_tx =
            crate::tenancy::scope::begin_tenant_system_scoped(&tenant_app, account_id)
                .await
                .unwrap();
        let (routed_mode, routed_graph): (Option<String>, Option<Uuid>) = sqlx::query_as(
            "SELECT mode, graph_id FROM google_chat_watched_spaces \
             WHERE account_id = $1 AND space_id = $2",
        )
        .bind(account_id)
        .bind("spaces/G46")
        .fetch_one(&mut *verify_tx)
        .await
        .expect("coralswarm_tenant_app must be able to read back the row its own write produced");
        verify_tx.rollback().await.ok();
        assert_eq!(routed_mode.as_deref(), Some("single"));
        assert_eq!(routed_graph, Some(team_graph_id));

        control_owner.close().await;
        control_app.close().await;
        tenant_owner.close().await;
        tenant_app.close().await;
        drop_db(&base, control_db).await;
        drop_db(&base, tenant_db).await;
    }
}
