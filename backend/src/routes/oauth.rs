//! OAuth callback handler and token management routes.
//!
//! # Routes
//!
//! | Method | Path                              | Auth     | Description                           |
//! |--------|-----------------------------------|----------|---------------------------------------|
//! | GET    | /v1/oauth/:provider/authorize     | public   | Redirect to provider consent URL      |
//! | GET    | /v1/oauth/:provider/callback      | public   | Exchange code, store encrypted token  |
//! | GET    | /v1/oauth/connections             | bearer   | List connected providers (no secrets) |
//! | DELETE | /v1/oauth/connections/:provider   | bearer   | Revoke a connection                   |
//!
//! # Key Security Properties
//!
//! - Access and refresh tokens are **never logged or returned** in API responses.
//! - Tokens are encrypted with AES-256-GCM using a per-account DEK before
//!   being stored in Postgres BYTEA columns.
//! - The per-account DEK is the SAME wrapped DEK content encryption uses
//!   (`accounts.dek_wrapped` / `account_keys.dek_wrapped`, wrapped under
//!   `MASTER_KEK_HEX`, resolved via [`crate::crypto::resolve_dek`] and cached
//!   in [`crate::dek_cache::DekCache`]) — Foundation PR 1.2 (CS-031) retired
//!   the earlier XOR-of-`ACCOUNT_MASTER_KEK`-with-the-account-UUID scheme
//!   from every request-path caller. `ACCOUNT_MASTER_KEK` survives only as
//!   [`crate::crypto::derive_legacy_oauth_account_dek`], read solely by the
//!   one-time cutover backfill (`bin/backfill_oauth_tokens_wrapped_dek`) that
//!   re-encrypts any row still in the old format.
//! - The OAuth `state` parameter is validated as a valid UUID to prevent CSRF;
//!   session-backed state is tracked in CS-110 (v1.1).

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use uuid::Uuid;

use coralswarm_core::oauth_token::{decrypt_token, encrypt_token};

use crate::{
    connectors::{self, catalog::ReturnTo, ConnectorError, TokenBundle, TokenOwner},
    crypto::DekPlane,
    middleware::auth::UserContext,
    oauth::state as oauth_state,
    tenancy::extractor::Tenant,
    AppState,
};

/// The `?surface=` value (and signed-state `sfc` claim) a Slack MEMBER
/// connect uses (CS-454, item 46a) — the same generic, provider-blind
/// `surface` mechanism gsuite Train G2/G4 built for Google's Gmail/Chat
/// (`MintStateParams::surface`, `oauth_state::sign_state`'s own `surface`
/// param takes a bare `&str`, not a `connectors::google::Surface`), reused
/// here rather than duplicated: a Slack authorize/callback round trip needs
/// the identical "this exact state was minted for THIS non-default flow,
/// carrying a connecting user id" property Gmail/Chat already rely on
/// (`mint_state`, `authorize`, `callback` each check this literal — see
/// their own comments). Never a client-supplied trust boundary by itself —
/// `callback` only ever trusts the value it reads back out of the VERIFIED,
/// HMAC-signed state, never `params.surface` directly.
pub(crate) const SLACK_MEMBER_SURFACE: &str = "slack_member";
/// The generic connector API's Slack connect when the workspace app is not
/// installed yet (connectors redesign, decision 4): ONE consent that asks for the
/// bot scopes and the member's own user scopes together, so whoever connects
/// first installs the app and signs themselves in. `callback` stores the bot row
/// (`slack`, account-wide) and the connector's `slack_user` row from the one
/// exchange. Once the app is installed, later members use [`SLACK_MEMBER_SURFACE`].
pub(crate) const SLACK_INSTALL_SURFACE: &str = "slack_install";

/// Fail-closed gate for the JWT-authed OAuth-connection routes under an org
/// `TenancyDecision` (org-tenancy PR 1.6, allowlist re-audit) — same policy
/// as `routes::connectors::reject_org_connector_access`. This matters most
/// for [`mint_state`]: the signed OAuth `state` binds the CALLER's
/// `account_id`, and the (unauthenticated, consumer-plane) [`callback`] later
/// stores the provider token under that account — so an org-context
/// `mint_state` would launder an ORG account id into a CONSUMER-plane
/// `oauth_tokens` write. Refusing here closes the whole flow at its only
/// authenticated entry point (the callback cannot mint its own state).
/// `list_connections`/`delete_connection` are gated for the same
/// plane-consistency reason. Personal / flag-off requests are byte-identical
/// to before. (This FILE stays on the grep-guard allowlist for its OPEN
/// provider-redirect machinery — authorize/admin_consent/callback carry no
/// Clerk JWT and resolve on the consumer plane by design.)
///
/// **org-slack step 5 — lifted, provider-BLIND, under [`org_slack_oauth_enabled_for`].**
/// [`mint_state`] cannot know which provider a caller is about to connect —
/// it is minted before the frontend even navigates to `/v1/oauth/{provider}/
/// authorize`, and its own route carries no `provider` parameter — so this
/// gate cannot make a per-provider decision either. Lifting it
/// unconditionally under the flag is still safe: minting a state token,
/// listing connections, and deleting connections touch no provider-specific
/// data by themselves (`authorize`/`admin_consent` were never gated here
/// either — see `routes::slack::oauth_start`'s identical "no gate" shape for
/// the same reason: no DB access, no write). The actual Slack-only
/// enforcement happens where the provider IS known — at the write point,
/// [`callback`]'s own org/provider check below — so a non-Slack row can
/// never exist for an org account regardless of what this gate allows
/// through. `list_connections`/`delete_connection` are therefore safe to
/// read/write unconditionally too: past this gate, they only ever see rows
/// `callback` was willing to create, i.e. Slack-only for an org account.
///
/// **Gmail carve-out lives in [`reject_org_gmail_management`], a SEPARATE
/// function (gsuite Train G2 PR 6, founder decision 2026-08-24 — mirrors
/// m365 Train 2 PR 2.5's `reject_org_teams_access`), not a branch added
/// here.** [`reject_org_chat_management`] is the identically-shaped Chat
/// sibling (gsuite Train G4 PR 6, "G4.6"). This function stays Slack-only
/// and byte-identical: [`mint_state`] DOES have a signal this function does
/// not use — its own `?surface=` query param — and calls
/// [`reject_org_gmail_management`]/[`reject_org_chat_management`] instead of
/// this function whenever that param parses to [`connectors::google::
/// Surface::Gmail`]/[`connectors::google::Surface::Chat`] respectively.
/// [`list_connections`]/[`delete_connection`] resolve ALL THREE flags
/// themselves (`org_slack_oauth_enabled_for`/`org_google_gmail_enabled_for`/
/// `org_google_chat_enabled_for` directly, OR-combined) rather than calling
/// this function at all — CodeRabbit round 1 on #695: calling this function
/// as a probe-then-Gmail-fallback logged ITS OWN "refused" `tracing::info!`
/// on every request that ultimately succeeded via the Gmail gate, since the
/// log fires before the caller ever consults the second gate; the identical
/// hazard applies to a Chat-only-enabled org against a probe call, which is
/// why Chat's arm (added by this PR) resolves its own flag directly too. See
/// either handler's own comment for the exact shape. "Slack-only for an org
/// account" in the paragraph above is the pre-PR-6 invariant for THIS
/// function specifically, not a statement about what those handlers allow as
/// of gsuite Train G2 PR 6 / G4 PR 6.
///
/// `async` (Foundation train PR 1.3): the flag decision now consults the
/// per-org registry ([`org_slack_oauth_enabled_for`]), which is a (cached)
/// control-plane lookup rather than a bare env-var read.
///
/// **Also checks the Microsoft registry flag (m365 enterprise train PR
/// 3.3).** Still provider-blind by construction (see this fn's own doc
/// comment above) — an org caller passes this gate the moment EITHER
/// provider's flag is on, and the provider-specific enforcement still
/// happens downstream where the provider IS known: [`callback`]'s own
/// org/provider gate for the write path, and each provider's own
/// connect/disconnect route for anything else (`routes::slack`'s
/// `reject_org_slack_non_admin` calls for Slack; Microsoft has no analogous
/// admin-only restriction — see [`mint_state`]'s own doc comment for why).
async fn reject_org_oauth_management(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if org_slack_oauth_enabled_for(state, &ctx.org_id).await
        || crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, &ctx.org_id).await
        // GitHub connector PR A: a single top-level flag (unlike Google's
        // per-surface split), so it joins this provider-blind OR-chain
        // directly — same shape as Slack/Microsoft above, not a separate
        // `reject_org_github_management` the way Gmail/Chat needed (their
        // own doc comments explain why THEY needed a surface-dispatched
        // sibling instead: `mint_state`'s `?surface=` param, which GitHub
        // has no equivalent of).
        || crate::connector_flags::github_enabled(state, &ctx.org_id).await
    {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "oauth connection route refused under org context (personal-workspace-only, or every \
         reachable provider's org flag is off for this org)"
    );
    Some(org_management_refused_response())
}

/// [`org_management_refused_response`]'s machine-readable `code`. The web's
/// first-run checklist reads it to tell "no connector is switched on for this
/// org" from a real failure, instead of matching the sentence in `error`.
pub(crate) const ORG_CONNECTORS_OFF_CODE: &str = "org_connectors_off";

/// The shared 403 body for every org-context connector-management refusal —
/// [`reject_org_oauth_management`], [`reject_org_gmail_management`], and
/// [`list_connections`]/[`delete_connection`]'s own combined-gate refusal
/// (CodeRabbit round 1 on #695) all return exactly this. Factored out so
/// `list_connections`/`delete_connection` can build the response AFTER
/// deciding the FINAL outcome across both gates, rather than trusting
/// either gate function's own bundled response (see those two handlers'
/// own comments for why that used to also log a spurious "refused" line).
/// It carries `code` = [`ORG_CONNECTORS_OFF_CODE`].
pub(crate) fn org_management_refused_response() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "connector authorization is available in your personal workspace only; \
                      switch out of the organization workspace to manage it",
            "code": ORG_CONNECTORS_OFF_CODE,
        })),
    )
        .into_response()
}

/// [`reject_org_oauth_management`]'s Gmail counterpart (gsuite Train G2 PR 6,
/// founder decision 2026-08-24 — mirrors m365 Train 2 PR 2.5's
/// `reject_org_teams_access`). Gated on [`org_google_gmail_enabled_for`]
/// (`ORG_GOOGLE_GMAIL_ENABLED` / the `org_connector_flags` registry,
/// provider key `google_gmail` — the SAME flag [`google_surface_enabled_for`]
/// already gates [`authorize`] on) instead of the Slack flag.
///
/// A SEPARATE function rather than a branch inside [`reject_org_oauth_management`]
/// (see that function's own doc comment): Gmail's per-member consent is not
/// an org-wide admin action the way a shared Slack workspace bot token is —
/// there is deliberately no [`reject_org_slack_non_admin`]-shaped check
/// layered on top of this one. Any org member may mint a Gmail state / see /
/// delete their OWN Gmail connection once their org's flag is on; only
/// [`callback`]'s own per-surface `TokenOwner::Member` resolution decides
/// whose row that is, never this gate.
async fn reject_org_gmail_management(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if org_google_gmail_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "oauth gmail management refused under org context (ORG_GOOGLE_GMAIL_ENABLED/\
         org_connector_flags resolved off for this org)"
    );
    Some(org_management_refused_response())
}

/// [`reject_org_gmail_management`]'s Chat counterpart (gsuite Train G4 PR 6,
/// "G4.6" — mirrors the gsuite Train G2 PR 6 Gmail org-plane lift,
/// `docs/connectors/google-workspace.md` §2). Gated on
/// [`org_google_chat_enabled_for`] (`ORG_GOOGLE_CHAT_ENABLED` /
/// `org_connector_flags`, provider key `google_chat`) instead of the Gmail
/// flag.
///
/// A SEPARATE function rather than a branch inside [`reject_org_oauth_management`]
/// or [`reject_org_gmail_management`], for the SAME reason those two are
/// already separate from each other: Chat's OAuth consent is per-member
/// (`Surface::Chat::token_owner_for` is always `Member`, built alongside
/// Gmail's in gsuite Train G2 PR 1/G4.1), never an org-wide admin action the
/// way a shared Slack workspace bot token is — there is deliberately no
/// [`reject_org_slack_non_admin`]-shaped check layered on top of this one.
/// Any org member may mint a Chat state / see / delete their OWN Chat
/// connection once their org's flag is on; only [`callback`]'s own
/// per-surface `TokenOwner::Member` resolution decides whose row that is,
/// never this gate. (Once a Chat connection exists, an ADMIN-only check DOES
/// apply one layer further in — `routes::google_chat`'s destination-pick
/// authorization, which decides who may ROUTE a watched space into a team
/// ocean; that is a different axis entirely from "may this member connect
/// their own Chat account at all", the question this gate answers.)
async fn reject_org_chat_management(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if org_google_chat_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "oauth chat management refused under org context (ORG_GOOGLE_CHAT_ENABLED/\
         org_connector_flags resolved off for this org)"
    );
    Some(org_management_refused_response())
}

/// [`reject_org_chat_management`]'s Meet counterpart (Meet org-plane lift).
/// Gated on [`org_google_meet_enabled_for`] (`ORG_GOOGLE_MEET_ENABLED` /
/// `org_connector_flags`, provider key `google_meet`).
///
/// Like Gmail's and Chat's, a SEPARATE function with no
/// [`reject_org_slack_non_admin`]-shaped check: Meet's consent is the
/// connecting member's OWN (`Surface::Meet::token_owner_for` is always
/// `Member`), not an org-wide admin action. Any org member may mint a Meet
/// state / see / delete their OWN Meet connection once their org's flag is
/// on; the transcripts that connection reads fan out only to members of the
/// SAME org, each into their own member-private ocean (see
/// `connectors::google_meet`'s module doc).
async fn reject_org_meet_management(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if org_google_meet_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "oauth meet management refused under org context (ORG_GOOGLE_MEET_ENABLED/\
         org_connector_flags resolved off for this org)"
    );
    Some(org_management_refused_response())
}

/// [`reject_org_meet_management`]'s Calendar counterpart (Google Calendar org
/// lift). Gated on [`org_google_calendar_enabled_for`]
/// (`ORG_GOOGLE_CALENDAR_ENABLED` / `org_connector_flags`, provider key
/// `google_calendar`).
///
/// A SEPARATE function with no [`reject_org_slack_non_admin`]-shaped check,
/// for the same reason as Gmail's/Chat's/Meet's: under an org Calendar's
/// consent is the connecting member's OWN (`Surface::calendar_token_owner`
/// is `Member` on the org plane), never an org-wide admin act. Any org member
/// may mint a Calendar state / see / delete their OWN Calendar connection
/// once their org's flag is on.
async fn reject_org_calendar_management(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if org_google_calendar_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "oauth calendar management refused under org context (ORG_GOOGLE_CALENDAR_ENABLED/\
         org_connector_flags resolved off for this org)"
    );
    Some(org_management_refused_response())
}

/// Pure parsing logic behind [`org_slack_oauth_enabled`], factored out so the
/// flag semantics are unit-testable without mutating process-global env
/// state — mirrors `workers::member_ocean_reconciler::parse_enabled_flag`.
fn parse_org_slack_oauth_flag(raw: Option<&str>) -> bool {
    matches!(raw, Some(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

/// org-slack step 5: whether an ORG account may install/manage a **Slack**
/// connector. Slack is the one provider whose token is a workspace-wide bot
/// token rather than a per-member credential, so it alone can be safely
/// keyed on the org's own `account_id` today — see [`callback`]'s own
/// doc comment for why Google/Microsoft (and any other future provider)
/// must keep refusing under an org regardless of this flag: their per-member
/// OAuth work (an `owner_user_id` column threaded through `TokenOwner`) is
/// not built, and an org's members sharing one `account_id` means per-member
/// tokens would collide on `oauth_tokens`' `(account_id, provider,
/// provider_workspace_id)` unique constraint — the newest write winning for
/// every member, a cross-member credential leak.
///
/// Same fail-closed contract as `workers::member_ocean_reconciler::
/// member_oceans_enabled`: `"1"` or a case-insensitive `"true"` is on; unset
/// or anything else (including a typo) is off. Read fresh on every call —
/// never cached at process start — so flipping `ORG_SLACK_OAUTH_ENABLED`
/// takes effect on the very next request, no redeploy.
///
/// **The fleet-wide DEFAULT, not the final answer.** Since Foundation train
/// PR 1.3, a per-org row in `org_connector_flags` can override this default
/// for one org at a time — see [`org_slack_oauth_enabled_for`], which is
/// what every enforcement point actually calls now. This raw env reader
/// survives as that registry's `env_default` argument (and as the value
/// [`org_slack_oauth_enabled_for`] falls back to when no control pool is
/// configured) — turning it on still lets every org WITHOUT an
/// override row connect Slack, unchanged from before this registry existed.
///
/// `pub(crate)`: `routes::slack::reject_org_slack_management` calls this
/// directly (rather than re-deriving the same env-var contract) so BOTH
/// Slack-OAuth surfaces — this generic connector path and the legacy
/// `routes/slack.rs` path — agree on "on" with zero drift risk.
pub(crate) fn org_slack_oauth_enabled() -> bool {
    parse_org_slack_oauth_flag(std::env::var("ORG_SLACK_OAUTH_ENABLED").ok().as_deref())
}

/// Whether org `clerk_org_id` may install/manage a **Slack** connector —
/// [`org_slack_oauth_enabled`]'s env default, overridable per-org via the
/// `org_connector_flags` registry (Foundation train PR 1.3). See
/// [`crate::connector_flags::resolve`] for the resolution order and caching
/// contract. `pub(crate)`: `routes::slack::reject_org_slack_management` calls
/// this directly for the same "one shared surface, zero drift" reason
/// [`org_slack_oauth_enabled`]'s own doc comment already gives.
pub(crate) async fn org_slack_oauth_enabled_for(state: &AppState, clerk_org_id: &str) -> bool {
    crate::connector_flags::resolve(state, clerk_org_id, "slack", org_slack_oauth_enabled()).await
}

/// Whether org `clerk_org_id` may complete/manage a **Gmail** connection
/// (gsuite Train G2 PR 6, founder decision 2026-08-24). Reuses the EXACT
/// registry key and env default [`google_surface_enabled_for`] already
/// resolves for [`authorize`] — `connectors::google::Surface::Gmail::
/// flag_provider()` (`"google_gmail"`) and `surface_env_default(Gmail)`
/// (`ORG_GOOGLE_GMAIL_ENABLED`) — so a single flag governs both "may this
/// caller START the Gmail OAuth flow" (`authorize`, gated since gsuite Train
/// G2 PR 1) and "may this org account COMPLETE/manage it"
/// ([`reject_org_gmail_management`]/[`callback`]'s own org/provider gate,
/// gated as of this PR). No new registry row shape, no new env var.
pub(crate) async fn org_google_gmail_enabled_for(state: &AppState, clerk_org_id: &str) -> bool {
    crate::connector_flags::resolve(
        state,
        clerk_org_id,
        connectors::google::Surface::Gmail.flag_provider(),
        connectors::google::surface_env_default(connectors::google::Surface::Gmail),
    )
    .await
}

/// Whether org `clerk_org_id` may complete/manage a **Chat** connection
/// (gsuite Train G4 PR 6, "G4.6" — mirrors the gsuite Train G2 PR 6 Gmail
/// org-plane lift, `docs/connectors/google-workspace.md` §2). Reuses the
/// EXACT registry key and env default [`google_surface_enabled_for`] already
/// resolves for [`authorize`] — `connectors::google::Surface::Chat::
/// flag_provider()` (`"google_chat"`) and `surface_env_default(Chat)`
/// (`ORG_GOOGLE_CHAT_ENABLED`) — so a single flag governs both "may this
/// caller START the Chat OAuth flow" (`authorize`, gated since gsuite Train
/// G4 PR 1) and "may this org account COMPLETE/manage it" (this function /
/// [`callback`]'s own org/provider gate / `routes::google_chat`'s own
/// management gate, all gated as of this PR). No new registry row shape, no
/// new env var. `pub(crate)`: `routes::google_chat::
/// reject_org_google_chat_management` calls this directly so the connect
/// flow and the watched-space management routes can never drift on which
/// flag governs an org's Chat access.
pub(crate) async fn org_google_chat_enabled_for(state: &AppState, clerk_org_id: &str) -> bool {
    crate::connector_flags::resolve(
        state,
        clerk_org_id,
        connectors::google::Surface::Chat.flag_provider(),
        connectors::google::surface_env_default(connectors::google::Surface::Chat),
    )
    .await
}

/// Whether org `clerk_org_id` may complete/manage a **Meet** connection
/// (Meet org-plane lift — mirrors [`org_google_gmail_enabled_for`] /
/// [`org_google_chat_enabled_for`] exactly, one surface later;
/// `docs/connectors/google-workspace.md` §2/§11). Reuses the EXACT registry
/// key and env default [`google_surface_enabled_for`] already resolves for
/// [`authorize`] — `connectors::google::Surface::Meet::flag_provider()`
/// (`"google_meet"`) and `surface_env_default(Meet)`
/// (`ORG_GOOGLE_MEET_ENABLED`) — so a single flag governs "may this caller
/// START the Meet OAuth flow", "may this org account COMPLETE/manage it", AND
/// "does `workers::google_meet_sweep::tick_org` sweep this org at all". No new
/// registry row shape, no new env var. **Default OFF** (the env default is
/// unset = off, same as Gmail's and Chat's): an org opts in via an
/// `org_connector_flags` row for provider `google_meet` or by setting
/// `ORG_GOOGLE_MEET_ENABLED`.
pub(crate) async fn org_google_meet_enabled_for(state: &AppState, clerk_org_id: &str) -> bool {
    crate::connector_flags::resolve(
        state,
        clerk_org_id,
        connectors::google::Surface::Meet.flag_provider(),
        connectors::google::surface_env_default(connectors::google::Surface::Meet),
    )
    .await
}

/// Whether org `clerk_org_id` may complete/manage a **Google Calendar**
/// connection (Google Calendar org lift — mirrors
/// [`org_google_gmail_enabled_for`]/[`org_google_chat_enabled_for`]/
/// [`org_google_meet_enabled_for`] exactly, one surface later; Calendar was
/// the last Google surface still refused unconditionally under an org).
///
/// Registry key `connectors::google::Surface::Calendar::flag_provider()`
/// (`"google_calendar"`, which already existed in the vocabulary but was
/// never consulted: Calendar is grandfathered ON for the PERSONAL plane and
/// `authorize` never reads a flag for it there) with
/// [`connectors::google::org_calendar_env_default`]
/// (`ORG_GOOGLE_CALENDAR_ENABLED`) as the env default — NOT
/// `surface_env_default(Calendar)`, which is `true` unconditionally and
/// would have silently opened every org. **Default OFF**: an org opts in via
/// an `org_connector_flags` row for provider `google_calendar` or by setting
/// `ORG_GOOGLE_CALENDAR_ENABLED`. One flag governs "may this caller START the
/// Calendar OAuth flow" ([`authorize`]), "may this org account COMPLETE/
/// manage it" ([`reject_org_calendar_management`]/[`callback`]), "may it
/// `POST /v1/connectors/google/sync`", the `/v1/calendar/*` read routes, and
/// "does `workers::google_calendar_sync` sweep this org at all".
pub(crate) async fn org_google_calendar_enabled_for(state: &AppState, clerk_org_id: &str) -> bool {
    crate::connector_flags::resolve(
        state,
        clerk_org_id,
        connectors::google::Surface::Calendar.flag_provider(),
        connectors::google::org_calendar_env_default(),
    )
    .await
}

/// [`callback`]'s org/provider gate decision (gsuite Train G2 PR 6), factored
/// out to a small async function so the full matrix is directly unit-testable
/// without a mocked `exchange_code` round trip — no existing test in this
/// file exercises `callback`'s successful-write path at all, for ANY
/// provider (see the tests next to [`resolve_google_token_target`] for the
/// same "test the decision, not the whole HTTP handler" precedent this
/// mirrors). `provider_id` is `connector.id()`; `state_surface` is the
/// VERIFIED state's own `sfc` claim (`callback`'s own `state_surface`), never
/// a client param.
///
/// * `provider_id == "slack"` — gated on [`org_slack_oauth_enabled_for`]
///   only (org-slack step 5, unchanged).
/// * `provider_id == "google"` AND `state_surface` parses to
///   [`connectors::google::Surface::Gmail`] — gated on
///   [`org_google_gmail_enabled_for`] only (gsuite Train G2 PR 6). Note this
///   is NOT OR'd with the Slack flag: an org with Slack enabled but Gmail
///   disabled must still refuse a Gmail callback, and vice versa — each
///   provider/surface pair is gated on its OWN flag exclusively.
/// * `provider_id == "google"` AND `state_surface` parses to
///   [`connectors::google::Surface::Chat`] — gated on
///   [`org_google_chat_enabled_for`] only (gsuite Train G4 PR 6, "G4.6" —
///   mirrors the Gmail arm immediately above; same non-OR rule, its own flag
///   exclusively).
/// * `provider_id == "google"` AND `state_surface` parses to
///   [`connectors::google::Surface::Meet`] — gated on
///   [`org_google_meet_enabled_for`] only (Meet org-plane lift; mirrors the
///   Gmail/Chat arms above, same non-OR rule, its own flag exclusively).
/// * `provider_id == "google"` AND `state_surface` is **absent, or parses to
///   [`connectors::google::Surface::Calendar`]** — gated on
///   [`org_google_calendar_enabled_for`] only (Google Calendar org lift).
///   Absent is Calendar by construction: `callback`'s own
///   [`resolve_google_token_target`] defaults a `sfc`-less state to Calendar,
///   so the gate must classify it the same way or a `sfc`-less state would
///   complete as Calendar under a gate that never asked the Calendar flag.
///   Same non-OR rule: its own flag exclusively.
/// * `provider_id == "microsoft"` — gated on
///   [`crate::routes::teams_ingest::org_microsoft_teams_enabled_for`] only
///   (m365 enterprise train PR 3.3: per-member Microsoft OAuth, built in
///   Train 3.2 — `resolve_microsoft_token_target` — is what makes this safe;
///   same one-provider-one-flag registry entry teams_ingest already uses).
///   Independent of the Slack, Gmail, and Chat flags, same non-OR rule as
///   above.
/// * Anything else (an unrecognized provider, or an UNPARSEABLE non-empty
///   surface on `provider_id == "google"`) — always `false`, flag or no flag
///   (see `org_slack_oauth_enabled`'s doc comment for the exact
///   cross-member-leak this prevents for `AccountWide`-shaped tokens).
async fn google_org_callback_allowed(
    state: &AppState,
    org_id: &str,
    provider_id: &str,
    state_surface: Option<&str>,
) -> bool {
    if provider_id == "slack" {
        return org_slack_oauth_enabled_for(state, org_id).await;
    }
    if provider_id == "microsoft" {
        return crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, org_id).await;
    }
    if provider_id == "github" {
        return crate::connector_flags::github_enabled(state, org_id).await;
    }
    // One-button Google Workspace connect: allowed iff at least one surface
    // in the state's own claim still has its org flag on. Per-surface
    // filtering happens in `google_workspace_connect::handle_callback`.
    if provider_id == "google"
        && connectors::google_workspace::is_workspace_surface(state_surface)
    {
        return crate::routes::google_workspace_connect::org_callback_allowed(
            state, org_id, state_surface,
        )
        .await;
    }
    let parsed_surface = state_surface.and_then(connectors::google::Surface::parse);
    if provider_id == "google" && parsed_surface == Some(connectors::google::Surface::Gmail) {
        return org_google_gmail_enabled_for(state, org_id).await;
    }
    if provider_id == "google" && parsed_surface == Some(connectors::google::Surface::Chat) {
        return org_google_chat_enabled_for(state, org_id).await;
    }
    if provider_id == "google" && parsed_surface == Some(connectors::google::Surface::Meet) {
        return org_google_meet_enabled_for(state, org_id).await;
    }
    // Calendar: an explicit `calendar` claim, or NO claim at all (the
    // pre-surface-split default `resolve_google_token_target` resolves to
    // Calendar). An unparseable non-empty claim is NOT Calendar here — it
    // stays refused, so a typo/forgery can never ride the Calendar flag.
    if provider_id == "google"
        && (parsed_surface == Some(connectors::google::Surface::Calendar) || state_surface.is_none())
    {
        return org_google_calendar_enabled_for(state, org_id).await;
    }
    false
}

// ---------------------------------------------------------------------------
// Org governance (2026-08-04 decision record) — piece 1: admin-only
// connect/disconnect
// ---------------------------------------------------------------------------

/// **Exception (decision 2026-10-29): the `slack_install` surface.** A connect
/// started through the generic connector API (`/v1/connectors/slack/connect`)
/// is open to any org member, because connections are individual only and Slack
/// needs the workspace app installed to deliver events at all, so whoever
/// connects installs it. This function and the callback's matching check still
/// apply to the older per-provider bot install (no surface) and to disconnecting
/// the workspace bot; Slack's own app-approval settings apply on Slack's side.
/// Microsoft admin consent, the GitHub org install and the Google Chat app keep
/// their admin gates.
///
/// Refuse an org-context Slack connect/disconnect action to anyone but an
/// org admin/owner — org governance decision (2026-08-04), piece 1. Personal
/// accounts are UNCHANGED: `tenant.org_context()` is `None` for them, so this
/// always returns `None` (no refusal) on the personal plane, byte-identical
/// to before this decision.
///
/// `role` MUST come from the already-resolved `UserContext::role`, never a
/// fresh `account_members` query — same convention
/// `routes::slack::channel_management`'s own doc comment documents for
/// `viewer_role`: `require_auth`'s org branch already re-checks membership
/// LIVE on every request (`middleware::auth::live_org_membership`), so a
/// demotion is already effective on the very next call by the time a handler
/// ever sees `UserContext`.
///
/// Admin vocabulary matches the EXISTING convention in this codebase
/// (`services::deletion::is_admin_role`, which itself matches
/// `routes::escrow::is_admin` / `routes::teams::invite_member`'s owner
/// check) rather than inventing a new one: exactly `'owner'` or `'admin'` is
/// an admin; everything else — in practice just the literal `'member'`
/// `account_members.role` CHECK constraint allows — is not.
/// Reused directly rather than re-implemented, so this gate can never drift
/// from the deletion gate's own definition of "admin".
pub(crate) fn reject_org_slack_non_admin(tenant: &Tenant, role: &str) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if crate::services::deletion::is_admin_role(role) {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        role = %role,
        "Slack connect/disconnect refused: caller is not an organization admin (org governance \
         decision 2026-08-04, piece 1)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "only an organization admin can connect or disconnect Slack"
            })),
        )
            .into_response(),
    )
}

// ---------------------------------------------------------------------------
// Org governance (2026-08-04 decision record) — piece 2: one workspace per
// org
// ---------------------------------------------------------------------------

/// Is `account_id` already connected to a DIFFERENT Slack workspace than
/// `new_workspace_id`? Checks BOTH `slack_integrations` (the legacy path's
/// own table, and the generic path's best-effort mirror —
/// `connectors::slack::upsert_slack_integrations_mirror`) and
/// `connector_installs` (the modern path's routing index) — an org account
/// should never have a row in either naming a workspace other than the one
/// it is actively connected to, but checking only one table would miss a
/// workspace that landed in the other via a path that skipped the mirror.
///
/// Returns the offending `(workspace_id, workspace_name)` — the name is
/// `None` when the only surviving evidence is a `connector_installs` row
/// (which does not carry a human-readable name).
///
/// **`begin_account_scoped`, not a bare pool read** (CodeRabbit round-one
/// review, MUST FIX): `slack_integrations` carries 0083's `account_isolation`
/// RLS policy, and prod's `coralswarm_app` role is NOT `BYPASSRLS` — an
/// unscoped read against an RLS-covered table under that role does not
/// silently return zero rows, it ERRORS (`unrecognized configuration
/// parameter "app.account_id"`, the exact failure mode this repo's own
/// `slack_watched_channels`/RLS incident history already documents). Before
/// this fix, EVERY org Slack connect would have hard-failed (500) at this
/// check the moment it ran in production. `connector_installs` itself
/// carries no RLS policy at all (migration `0121_connector_installs`'s own
/// "No RLS, deliberately" header — a webhook resolver needs unscoped
/// cross-account reads), so it does not strictly need the scope, but sharing
/// the one transaction with the `slack_integrations` read costs nothing.
async fn find_other_org_slack_workspace(
    control_pool: &PgPool,
    account_id: Uuid,
    new_workspace_id: &str,
) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| {
            sqlx::Error::Protocol(format!("find_other_org_slack_workspace scoped tx: {e}"))
        })?;

    if let Some((workspace_id, workspace_name)) = sqlx::query_as::<_, (String, String)>(
        "SELECT workspace_id, workspace_name FROM slack_integrations \
         WHERE account_id = $1 AND workspace_id <> $2 LIMIT 1",
    )
    .bind(account_id)
    .bind(new_workspace_id)
    .fetch_optional(&mut *tx)
    .await?
    {
        tx.rollback().await.ok();
        return Ok(Some((workspace_id, Some(workspace_name))));
    }

    if let Some((workspace_id,)) = sqlx::query_as::<_, (String,)>(
        "SELECT external_workspace_id FROM connector_installs \
         WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id <> $2 LIMIT 1",
    )
    .bind(account_id)
    .bind(new_workspace_id)
    .fetch_optional(&mut *tx)
    .await?
    {
        tx.rollback().await.ok();
        return Ok(Some((workspace_id, None)));
    }

    tx.rollback().await.ok();
    Ok(None)
}

/// Same two-table lookup as [`find_other_org_slack_workspace`], against an
/// already-open transaction instead of a bare pool — see
/// [`refuse_second_org_slack_workspace_locked`]'s own doc comment for why a
/// SECOND copy of this query, run inside the write transaction, is what
/// actually closes the race the pool-based check alone cannot.
async fn find_other_org_slack_workspace_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    new_workspace_id: &str,
) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
    if let Some((workspace_id, workspace_name)) = sqlx::query_as::<_, (String, String)>(
        "SELECT workspace_id, workspace_name FROM slack_integrations \
         WHERE account_id = $1 AND workspace_id <> $2 LIMIT 1",
    )
    .bind(account_id)
    .bind(new_workspace_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        return Ok(Some((workspace_id, Some(workspace_name))));
    }

    if let Some((workspace_id,)) = sqlx::query_as::<_, (String,)>(
        "SELECT external_workspace_id FROM connector_installs \
         WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id <> $2 LIMIT 1",
    )
    .bind(account_id)
    .bind(new_workspace_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        return Ok(Some((workspace_id, None)));
    }

    Ok(None)
}

/// Turn a [`find_other_org_slack_workspace`]/[`find_other_org_slack_workspace_tx`]
/// result into the refusal response both the pool-based pre-check and the
/// transaction-scoped recheck return — kept as one function so the two
/// call sites can never drift on the error message or status code.
fn refuse_workspace_conflict(
    new_workspace_id: &str,
    found: Option<(String, Option<String>)>,
) -> Result<(), Response> {
    match found {
        None => Ok(()),
        Some((existing_workspace_id, existing_workspace_name)) => {
            tracing::info!(
                existing_workspace_id = %existing_workspace_id,
                new_workspace_id = %new_workspace_id,
                "org Slack connect refused: this account already has a different Slack workspace \
                 connected (one workspace per org — org governance decision 2026-08-04, piece 2)"
            );
            let label = existing_workspace_name.unwrap_or_else(|| existing_workspace_id.clone());
            Err((
                StatusCode::CONFLICT,
                Json(json!({
                    "error": format!(
                        "this organization is already connected to the Slack workspace \"{label}\"; \
                         disconnect it before connecting a different workspace"
                    )
                })),
            )
                .into_response())
        }
    }
}

/// Refuse to write a SECOND Slack workspace for an org account — org
/// governance decision (2026-08-04), piece 2. Org accounts only, per the
/// decision; a personal account is free to connect more than one workspace
/// (a pre-existing edge case this decision does not touch — see
/// `routes::slack::resolve_connected_slack_workspace`'s own doc comment for
/// how that case already behaves).
///
/// Called BEFORE any row is written for the new workspace — from BOTH Slack
/// OAuth callbacks that can write for an org account (this file's
/// [`callback`] and the legacy `routes::slack::oauth_callback`), so an org
/// can never end up with two workspaces regardless of which path a request
/// takes. This is what kills `resolve_connected_slack_workspace`'s
/// `ORDER BY created_at DESC LIMIT 1` shadowing BY CONVENTION for the single-
/// request case — see [`refuse_second_org_slack_workspace_locked`]'s own doc
/// comment for the genuinely CONCURRENT case this function alone does not
/// close.
///
/// **This check and the write it guards run on SEPARATE connections/
/// transactions** (this is a plain `SELECT` against `control_pool`, called
/// before `callback`/`oauth_callback` ever open their own write transaction)
/// — a classic check-then-write TOCTOU: two requests connecting two
/// DIFFERENT workspaces for the SAME org, arriving concurrently, can both
/// observe "nothing else connected yet" and both proceed to write, landing
/// two `slack_integrations` rows with no unique constraint to reject the
/// second (no migration in this PR — see [`refuse_second_org_slack_workspace_locked`]
/// for the actual close of this window, which callers run a second time,
/// inside their write transaction).
pub(crate) async fn refuse_second_org_slack_workspace(
    control_pool: &PgPool,
    account_id: Uuid,
    new_workspace_id: &str,
) -> Result<(), Response> {
    match find_other_org_slack_workspace(control_pool, account_id, new_workspace_id).await {
        Ok(found) => refuse_workspace_conflict(new_workspace_id, found),
        Err(e) => {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                new_workspace_id = %new_workspace_id,
                "failed to check for an existing org Slack workspace before install"
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response())
        }
    }
}

/// The TOCTOU close for [`refuse_second_org_slack_workspace`]: re-runs the
/// SAME check a second time, but on the caller's own already-open write
/// transaction, after first taking a per-account Postgres advisory lock —
/// `pg_advisory_xact_lock`, the SAME blocking, transaction-scoped primitive
/// `migrations_control/0083_consumer_rls.sql`'s own DDL guard and
/// `workers::journal_sweep`'s plane lock already rely on (see either's own
/// doc comment for the "released automatically at COMMIT/ROLLBACK, no
/// separate unlock call" contract this reuses verbatim).
///
/// Deliberately the BLOCKING variant (`pg_advisory_xact_lock`, not
/// `pg_try_advisory_xact_lock`): this guards a low-frequency, human-driven
/// action (one OAuth callback per connect click), never a hot loop, so
/// making a second concurrent request for the same org wait for the first to
/// finish and then getting a CORRECT refusal is strictly better than a fast
/// wrong answer (two workspaces landed). Two DIFFERENT orgs never contend —
/// the lock key is namespaced per `account_id`.
///
/// No migration: this closes the race without a unique constraint, which is
/// the more complete fix but out of scope for this PR (no migrations). A
/// unique constraint would additionally cover a write that reaches
/// `slack_integrations` through some future path that skips this function
/// entirely; that residual gap is accepted for now.
pub(crate) async fn refuse_second_org_slack_workspace_locked(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    new_workspace_id: &str,
) -> Result<(), Response> {
    if let Err(e) =
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('slack_org_workspace:' || $1::text))")
            .bind(account_id.to_string())
            .execute(&mut **tx)
            .await
    {
        tracing::error!(
            error = %e,
            account_id = %account_id,
            "failed to take the per-account Slack-connect advisory lock"
        );
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response());
    }

    match find_other_org_slack_workspace_tx(tx, account_id, new_workspace_id).await {
        Ok(found) => refuse_workspace_conflict(new_workspace_id, found),
        Err(e) => {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                new_workspace_id = %new_workspace_id,
                "failed to re-check for an existing org Slack workspace inside the write \
                 transaction"
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response())
        }
    }
}

// ---------------------------------------------------------------------------
// Query parameter types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct AuthorizeParams {
    // Optional: allow caller to pass a custom state (CSRF token).
    // When absent we generate one.
    pub state: Option<String>,
    /// Google only (gsuite Train G1 PR 1): which surface's scope to request
    /// — `"calendar"` (default when absent, preserving every existing
    /// caller's behavior byte-for-byte), `"gmail"`, `"chat"`, or `"meet"`.
    /// Ignored by every other provider. See [`connectors::google::Surface`]
    /// and [`authorize`]'s own doc.
    pub surface: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CallbackParams {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
    /// Present only on Microsoft's org-wide admin-consent redirect (Train
    /// 3.2) — `prompt=admin_consent` on the standard `/authorize` endpoint
    /// (`MicrosoftConnector::admin_consent_url`) returns `admin_consent=True
    /// &tenant=<id>&scope=...` instead of a `code`. Ignored by every other
    /// provider and by an ordinary Microsoft per-user consent completion.
    /// See [`callback`]'s own admin-consent branch.
    pub admin_consent: Option<String>,
    /// The Azure AD tenant id Microsoft consented on behalf of — present
    /// only alongside `admin_consent`.
    pub tenant: Option<String>,
    /// The space-separated scope set Microsoft reports as granted — present
    /// only alongside `admin_consent`. Stored verbatim for audit (did the
    /// admin actually approve the full frozen scope set this app requested?).
    pub scope: Option<String>,
    /// GitHub App install callback only (docs/connectors/github.md §3.1):
    /// `request_oauth_on_install=true` means this callback ALSO carries the
    /// numeric installation id the user just granted/reconfigured, alongside
    /// the ordinary user-to-server `code`. Ignored by every other provider.
    pub installation_id: Option<String>,
    /// GitHub App install callback only: `"install"` | `"update"` — which
    /// kind of install-screen round trip this was. Ignored by every other
    /// provider.
    pub setup_action: Option<String>,
}

/// Query params for [`mint_state`] (gsuite Train G2 PR 1, item 3).
#[derive(Debug, Deserialize)]
pub struct MintStateParams {
    /// Optionally request that the minted state embed a specific Google
    /// surface (e.g. `"gmail"`) as its `sfc` claim. See [`mint_state`]'s own
    /// doc for exactly what this does and does not validate.
    pub surface: Option<String>,
}

// ---------------------------------------------------------------------------
// Response types (no token material)
// ---------------------------------------------------------------------------

/// A single OAuth connection record returned by the list endpoint.
/// Token material is intentionally excluded.
#[derive(Debug, Serialize)]
pub struct OAuthConnection {
    pub provider: String,
    pub provider_workspace_id: Option<String>,
    pub scope: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub connected_at: DateTime<Utc>,
    /// CS-431 (org-onboarding-from-the-invite.md §3 item 34) — scopes this
    /// codebase currently requests for `provider` that are absent from
    /// `scope` above. Computed only for `"slack"` (against
    /// `connectors::slack::slack_scopes()`) and `"google_chat"` (against
    /// `connectors::google::Surface::Chat::data_scope()`); every other
    /// provider always reports `[]` here — there is no known-stale-grant
    /// case for it yet. See [`scope_reconsent_status`].
    pub missing_scopes: Vec<String>,
    /// CS-431 — `true` when `scope` cannot be trusted to answer "is
    /// anything missing" at all (e.g. a Slack workspace migrated from the
    /// legacy `slack_integrations`-only path, which never recorded a real
    /// scope string). `missing_scopes` is always `[]` when this is `true` —
    /// never guessed together. See [`scope_reconsent_status`].
    pub scopes_unknown: bool,
    /// When a definite revocation was recorded against this row
    /// (`connectors::credential_health`) — the connection is dead until the
    /// user reconnects, and the reconnect clears it. The connectors page's
    /// reconnect notice reads this, for every provider and both planes:
    /// it is the only member-facing surface for a dead credential (the
    /// `connector_health` record is an admin's, for SHARED capabilities
    /// only). State, not an event, so it is shown once however many sync
    /// passes observe the death — and the sweeps stop retrying a marked row.
    pub credential_invalid_at: Option<DateTime<Utc>>,
    /// `CredentialDeathReason::code()` of that revocation; `None` exactly
    /// when `credential_invalid_at` is.
    pub credential_invalid_reason: Option<String>,
    /// `true` when this row is the caller's own per-member grant
    /// (`owner_user_id` = the caller), `false` for an account-wide row. The
    /// listing returns both, and under D21 only the first means "you
    /// connected this"; the web's first-run checklist and the roster's
    /// `connected_providers` both read it that way.
    pub mine: bool,
}

/// [`OAuthConnection::missing_scopes`]/`scopes_unknown` for one connection
/// row — CS-431. The two provider literals this actually computes for are
/// the two grants D17 point 5 (org-onboarding-from-the-invite.md) names as
/// able to go silently stale forever: a Slack bot token installed before
/// `users:read.email` shipped, and a Google Chat grant made before
/// `directory.readonly` shipped. Every other provider gets `([], false)`
/// unconditionally — computing a missing-scope diff for a provider with no
/// known stale-grant case would be exactly the kind of guess this feature
/// exists to refuse.
pub(crate) fn scope_reconsent_status(provider: &str, stored_scope: &str) -> (Vec<String>, bool) {
    match provider {
        "slack" => match connectors::slack::slack_missing_scopes(stored_scope) {
            Some(missing) => (missing, false),
            None => (Vec::new(), true),
        },
        "google_chat" => match connectors::google::chat_missing_scopes(stored_scope) {
            Some(missing) => (missing, false),
            None => (Vec::new(), true),
        },
        _ => (Vec::new(), false),
    }
}

// ---------------------------------------------------------------------------
// GET /v1/oauth/:provider/authorize
// ---------------------------------------------------------------------------

/// Redirect the user to the provider's OAuth consent page.
///
/// Dispatches to the per-provider [`ConnectorProvider`](crate::connectors::ConnectorProvider)
/// strategy. Unknown provider → `404`; registered-but-unimplemented (e.g. google)
/// → `501`; configured provider with missing env → `503`.
///
/// # Google surface gate (gsuite Train G1 PR 1)
/// For `provider == "google"`, `?surface=` selects which surface's scope to
/// request (default `"calendar"` — every existing caller that never passes
/// this param gets byte-identical behavior to before this PR). Requesting a
/// non-Calendar surface (`gmail`/`chat`/`meet`) is gated on
/// [`google_surface_enabled_for`]: unknown surface → `400`; disabled surface
/// → `403`. This route stays otherwise unauthenticated (see the module doc
/// on why `authorize`/`admin_consent`/`callback` carry no Clerk JWT and no
/// `Tenant` extractor) — the gate resolves the caller's account/org
/// best-effort from the (optional, here-non-consuming) signed `state` token,
/// the same way [`callback`] resolves it for its own org/provider gate below,
/// and falls back to the raw env default when `state` is absent or
/// unverifiable (i.e. every caller that hasn't already run [`mint_state`]) —
/// this is a pure ADDITIVE gate on the three inert surfaces; it does not
/// change Calendar's behavior and does not touch `callback`'s own
/// unconditional "only Slack may install under an org" refusal.
///
/// **Even a flag-enabled non-Calendar surface still refuses — with `501`,
/// distinct from the flag's own `403` — until [`connectors::google::Surface::
/// is_implemented`] is true for it.** Gmail's gate lifted in gsuite Train G2
/// PR 1; Chat's lifted in gsuite Train G4 PR 1 ("G4.1"): `callback` now
/// resolves a real per-surface `(TokenOwner, provider_key)` for either
/// (never `TokenOwner::AccountWide` under the Calendar-shared `"google"`
/// literal), so a completed Gmail or Chat consent can no longer collide
/// with an existing Calendar-scoped token. **Meet still `501`** — no
/// ingest code, no reachable OAuth completion. Chat is, as of G4.1, in the
/// same "no ingest code reads the token yet" position Meet is in — Chat's
/// ingest lands in later gsuite Train G4 PRs (3/4) — but its gate lifts
/// anyway, deliberately: G4.1's whole point is amending Chat's frozen scope
/// set (`connectors::google::Surface::Chat::data_scope`) BEFORE any member
/// has ever consented to `google_chat`, which is the only moment that
/// amendment is free (see that function's own doc comment, and
/// `.scratch/gsuite-enterprise/g4-chat-design.md` §0). A completed Chat
/// consent under this PR just stores an inert, correctly-isolated
/// per-member token — see [`connectors::google::Surface::is_implemented`]'s
/// own doc comment for why that is safe.
///
/// **Gmail, and as of G4.1 also Chat, additionally require a verifiable
/// signed `state`** (gsuite Train G2 PR 1, item 4 — extended to Chat by
/// G4.1, not a new mechanism): [`require_verified_surface_state`] rejects
/// (`400`) a bare/random/mismatched `state` — one that does not itself embed
/// this exact surface and a connecting user id — before ever redirecting to
/// Google. This is a fail-fast UX check, not the security boundary: even if
/// it were skipped, `callback` independently re-verifies the state and
/// refuses (never falls back to `TokenOwner::AccountWide`) a non-Calendar
/// surface whose state has no embedded user id — see [`callback`]'s own
/// comment. Chat needs this guard for the same reason Gmail does: per-member
/// storage (`Surface::Chat::token_owner_for` is always `Member`) has nowhere
/// safe to put a token with no verified owner, so there is no silent
/// `AccountWide` fallback available to fall back to.
pub async fn authorize(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Query(params): Query<AuthorizeParams>,
) -> Response {
    let connector = match connectors::lookup(&provider) {
        Some(c) => c,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("unknown provider: {provider}")})),
            )
                .into_response();
        }
    };

    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());

    // Use caller-supplied state or generate a random UUID (CSRF token / account id).
    let state_val = params.state.unwrap_or_else(|| Uuid::new_v4().to_string());

    if provider == "google" {
        // One-button Google Workspace connect: its own scope union and gates,
        // handled entirely in `google_workspace_connect::authorize`.
        if connectors::google_workspace::is_workspace_surface(params.surface.as_deref()) {
            return crate::routes::google_workspace_connect::authorize(
                &state, &state_val, &base_url,
            )
            .await;
        }
        let surface = match params.surface.as_deref() {
            None => connectors::google::Surface::Calendar,
            Some(raw) => match connectors::google::Surface::parse(raw) {
                Some(s) => s,
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("unknown google surface: {raw}")})),
                    )
                        .into_response();
                }
            },
        };

        if !surface.is_grandfathered() {
            if !google_surface_enabled_for(&state, &state_val, surface).await {
                tracing::info!(
                    surface = surface.as_str(),
                    "google authorize refused: surface not enabled (ORG_GOOGLE_* flag off)"
                );
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({
                        "error": format!(
                            "the {} surface is not enabled for this workspace yet",
                            surface.as_str()
                        )
                    })),
                )
                    .into_response();
            }
            // CodeRabbit #669 finding (verified): before gsuite Train G2 PR 1,
            // `callback` had no way to retain WHICH surface an authorize
            // request was for — a completed OAuth flow always persisted as
            // the single generic `google` `oauth_tokens` row, so a real
            // Gmail-scoped consent could have silently overwritten an
            // existing Calendar-scoped token for the same account. Gmail's
            // gate lifted (G2 PR 1) once BOTH (a) it had real per-surface
            // token isolation (`Surface::token_provider`/`token_owner_for`,
            // `callback`'s own resolution logic) and (b) `Surface::
            // is_implemented` reflects that. Chat's gate lifts here too, as
            // of gsuite Train G4 PR 1 ("G4.1") — same two conditions,
            // already true for Chat since G2.1 built the per-surface
            // plumbing generically (`Surface::ALL`, not a Gmail special
            // case). Meet still refuses here — no ingest code exists for
            // it, so completing its OAuth flow would just produce a token
            // nothing ever reads; Chat is in that same "no ingest code
            // reads it" position as of G4.1, but its gate lifts anyway
            // because amending its frozen scope set only before any member
            // has consented is the entire point of this PR (see
            // `Surface::Chat::data_scope`'s doc comment). The flag gate
            // above stays meaningful (and testable) on its own; this is
            // a SEPARATE, unconditional check layered on top of it.
            if !surface.is_implemented() {
                tracing::info!(
                    surface = surface.as_str(),
                    "google authorize refused: surface not implemented yet \
                     (no ingest code reads a token for it)"
                );
                return (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(json!({
                        "error": format!(
                            "the {} surface is not available yet",
                            surface.as_str()
                        )
                    })),
                )
                    .into_response();
            }
            // Item 4 (gsuite Train G2 PR 1): an implemented, non-grandfathered
            // surface (Gmail today) may only proceed with a `state` that is
            // ITSELF a verifiable signed token whose own embedded claims
            // already declare this exact surface plus a connecting user id —
            // never a bare/random/unsigned/mismatched one. Without this, a
            // caller could mint a plain (no `?surface=`) state via
            // `mint_state`, then tack `?surface=gmail` onto THIS request only
            // — `callback` would later see a verified-but-claim-less state
            // and (byte-identical-to-Calendar default) resolve it as
            // Calendar/AccountWide, completing a real Gmail consent onto the
            // Calendar-scoped row. See `require_verified_surface_state`'s own
            // doc for why this is fail-fast UX, not the security boundary
            // (`callback` re-verifies independently either way).
            if let Err(resp) = require_verified_surface_state(&state_val, surface) {
                return resp;
            }
            // CodeRabbit round 1 on gsuite Train G4 PR 1 ("G4.1"): refuse a
            // redirect here that `callback`'s own org/provider gate
            // (`google_org_callback_allowed`, unchanged by this PR — no new
            // arm added) would only refuse anyway. Without this, an
            // org-plane caller whose account happens to already have SOME
            // implemented-and-org-callback-allowed Google surface's flag on
            // (Gmail's, e.g.) can reach `mint_state` (its own else-branch
            // gate is provider-blind — see `reject_org_oauth_management`'s
            // own doc), request `?surface=chat`, and — once `ORG_GOOGLE_
            // CHAT_ENABLED` is ALSO on for that org — sail straight through
            // every check above into a REAL Google consent redirect, only to
            // have `callback` 403 them moments later with no `oauth_tokens`
            // row ever written: a silently wasted, real external OAuth
            // grant, not just an inconvenient error page. Consulting the
            // SAME function `callback` uses — rather than inventing a
            // second, independently-maintained rule that could drift from
            // it — means `authorize` can never promise a redirect `callback`
            // is guaranteed to refuse. This does NOT lift, weaken, or add an
            // arm to the org gate itself (`google_org_callback_allowed`
            // stays untouched; `reject_org_connector_access` stays
            // untouched) — it only moves the SAME refusal earlier for a
            // caller who would otherwise complete a doomed round trip.
            // Gmail is a no-op here (its own arm already returns `true` when
            // its flag is on); Calendar never reaches this branch. Meet
            // (Meet org-plane lift) is exactly the Chat shape: this check
            // consults `google_org_callback_allowed`'s Meet arm, so an org
            // whose `google_meet` flag is off is refused HERE (before any
            // live Google consent), and an org whose flag is on passes
            // through to the redirect.
            if let Some(resp) =
                refuse_doomed_org_authorize_redirect(&state, &state_val, surface).await
            {
                return resp;
            }
            return match connectors::google::authorize_url_for_surface(
                &base_url, &state_val, surface,
            ) {
                Ok(url) => Redirect::temporary(&url).into_response(),
                Err(e) => connector_error_response(e),
            };
        }
        // Calendar: falls through to the generic redirect below — after ONE
        // org-only pre-redirect check (Google Calendar org lift). Calendar is
        // grandfathered ON for the personal plane, so no flag is consulted
        // for it here; but under an ORG account `callback`'s own gate
        // (`google_org_callback_allowed`) refuses Calendar unless the org's
        // `google_calendar` flag is on, and redirecting a flag-off org
        // through a live Google consent only to 403 it afterwards would waste
        // a real external grant (the exact scenario
        // `refuse_doomed_org_authorize_redirect` exists for). Fails OPEN on a
        // personal-plane/unverifiable state, so every existing personal
        // Calendar connect is byte-identical.
        if let Some(resp) =
            refuse_doomed_org_authorize_redirect(&state, &state_val, connectors::google::Surface::Calendar)
                .await
        {
            return resp;
        }
    } else if provider == "slack"
        && matches!(
            params.surface.as_deref(),
            Some(SLACK_MEMBER_SURFACE) | Some(SLACK_INSTALL_SURFACE)
        )
    {
        // CS-454, item 46a: a MEMBER (user-scope-only) Slack connect —
        // deliberately NOT the generic `connector.authorize_url` fallthrough
        // below, which builds the admin bot-install URL (`scope=` + optional
        // `user_scope=`). This builds `user_scope=`-ONLY, no `scope=` at all
        // (`connectors::slack::authorize_url_for_member`), which is what
        // lifts the admin gate — see that function's own doc comment.
        //
        // Same fail-fast UX check Gmail/Chat get from `require_verified_
        // surface_state` (not reused directly — that function is typed to
        // `connectors::google::Surface`, which has no Slack variant): the
        // state must ALREADY verify and ALREADY declare this exact surface
        // plus a connecting user id, before redirecting to a live Slack
        // consent screen. This is NOT the security boundary — `callback`
        // independently re-verifies and refuses (never falls back to
        // `TokenOwner::AccountWide`) a `slack_member` surface with no
        // embedded user id — only a wasted-round-trip guard, same as
        // `require_verified_surface_state`'s own doc comment states for
        // itself.
        let secret = match oauth_state::state_secret() {
            Some(s) => s,
            None => {
                tracing::error!("OAUTH_STATE_SECRET missing or empty");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "oauth state signing not configured"})),
                )
                    .into_response();
            }
        };
        let mismatch_response = || {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "the slack_member surface requires a freshly minted signed state \
                              for this surface — reconnect and try again"
                })),
            )
                .into_response()
        };
        let verified = match oauth_state::verify_state(&secret, &state_val) {
            Ok(v) => v,
            Err(_) => return mismatch_response(),
        };
        // The request's `?surface=` and the state's own `sfc` claim must name the
        // same flow: a member state cannot be replayed as an install, or the
        // reverse.
        let requested = params.surface.as_deref().unwrap_or_default();
        if verified.user_id.is_none() || verified.surface.as_deref() != Some(requested) {
            tracing::info!(
                "slack member authorize refused: state does not declare the requested slack \
                 surface and a connecting user id"
            );
            return mismatch_response();
        }
        let built = if requested == SLACK_INSTALL_SURFACE {
            connectors::slack::authorize_url_for_install(&base_url, &state_val)
        } else {
            connectors::slack::authorize_url_for_member(&base_url, &state_val)
        };
        return match built {
            Ok(url) => Redirect::temporary(&url).into_response(),
            Err(e) => connector_error_response(e),
        };
    }

    match connector.authorize_url(&base_url, &state_val) {
        Ok(url) => Redirect::temporary(&url).into_response(),
        Err(e) => connector_error_response(e),
    }
}

/// [`authorize`]'s pre-redirect consultation of `callback`'s own org/
/// provider gate (gsuite Train G4 PR 1, "G4.1", CodeRabbit round 1) — see
/// the call site's own comment for the full scenario this closes. Returns
/// `Some(refusal)` only when ALL of the following hold: the state verifies,
/// it resolves to an account with a `clerk_org_id` (org plane), AND
/// [`google_org_callback_allowed`] — called with `provider_id = "google"`
/// and this exact surface, EXACTLY the arguments `callback` will use for
/// this same state moments later — returns `false` for it.
///
/// Deliberately fails OPEN (returns `None`, i.e. "don't refuse here") on
/// every ambiguous case, the same posture [`google_surface_enabled_for`]
/// itself takes for the analogous flag lookup: no DB configured, no state
/// secret configured, an unverifiable state, no matching `accounts` row, a
/// personal-plane account (`clerk_org_id` NULL), or a query error. This is
/// safe specifically BECAUSE it is an ADDITIVE, non-exclusive check layered
/// in front of a redirect that is otherwise harmless to attempt — every one
/// of those "don't know" cases is either the well-trodden personal-plane
/// path (unaffected) or a state so broken `callback` will refuse it anyway
/// on its own, independent verification. This function never consumes the
/// state's `jti` (verification only, like [`require_verified_surface_state`]
/// and [`google_surface_enabled_for`] before it) and performs a read-only
/// query, so calling it costs nothing on the paths it does not refuse.
async fn refuse_doomed_org_authorize_redirect(
    state: &AppState,
    oauth_state_val: &str,
    surface: connectors::google::Surface,
) -> Option<Response> {
    let pool = state.db.as_ref()?;
    let secret = oauth_state::state_secret()?;
    let verified = oauth_state::verify_state(&secret, oauth_state_val).ok()?;

    let row: Result<Option<Option<String>>, sqlx::Error> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(verified.account_id)
            .fetch_optional(pool)
            .await;
    let org_id = match row {
        Ok(Some(Some(org_id))) => org_id,
        // No account row, or an account with no clerk_org_id (personal
        // plane) — nothing to refuse against; fail open exactly like
        // `google_surface_enabled_for` does for its own lookup.
        Ok(_) => return None,
        // CodeRabbit round 2 on gsuite Train G4 PR 1 ("G4.1"): a query
        // error is NOT the same as "personal plane, nothing to refuse
        // against" — silently folding it into that arm would hide a real
        // DB failure. Same log shape as `google_surface_enabled_for`'s own
        // error arm just above. Still fails OPEN (returns `None`, i.e.
        // "don't refuse here") on a transient DB error: this is an
        // ADDITIVE pre-redirect check layered in front of a flow `callback`
        // independently re-verifies, so the worst case on a transient
        // failure here is the pre-fix bad UX (a live redirect callback then
        // 403s) — never a security regression, since callback's own gate is
        // untouched and still authoritative.
        Err(e) => {
            tracing::error!(
                error = %e,
                surface = surface.as_str(),
                "authorize: pre-redirect org gate account lookup failed; failing open \
                 (not refusing here — callback re-verifies independently either way)"
            );
            return None;
        }
    };

    if google_org_callback_allowed(state, &org_id, "google", Some(surface.as_str())).await {
        None
    } else {
        tracing::info!(
            org_id = %org_id,
            surface = surface.as_str(),
            "google authorize refused BEFORE redirecting: callback's own org/provider gate \
             would refuse this exact state anyway — refusing now rather than sending the \
             caller through a live Google consent round trip guaranteed to end in a 403 with \
             no oauth_tokens row ever written"
        );
        Some(org_management_refused_response())
    }
}

/// Item 4 (gsuite Train G2 PR 1): require that `state_val` verifies AND its
/// own embedded claims already declare `surface` plus a connecting user id,
/// before [`authorize`] redirects to Google for a non-grandfathered,
/// implemented surface (Gmail today). `400` for anything short of that
/// (missing, malformed, unsigned, expired, tampered, wrong-surface, or
/// no-embedded-user-id state) — deliberately the same generic "invalid
/// state"-shaped error `callback` uses for its own state failures, so this
/// is not a distinguishing oracle. `503` only for the one true server
/// misconfiguration case (`OAUTH_STATE_SECRET` unset).
///
/// Pure fail-fast UX: `verify_state` does no DB access and this function
/// does not consume the state's `jti` (that stays the single-use choke point
/// inside `callback`, run again independently there). Skipping this check
/// entirely would not open a hole — `callback` performs the identical
/// surface/user-id resolution itself and refuses (never falls back to
/// `TokenOwner::AccountWide`) rather than trust an unverified or claim-less
/// state for a non-Calendar surface. This function only saves the caller a
/// wasted round trip through Google's consent screen.
fn require_verified_surface_state(
    state_val: &str,
    surface: connectors::google::Surface,
) -> Result<(), Response> {
    let secret = match oauth_state::state_secret() {
        Some(s) => s,
        None => {
            tracing::error!("OAUTH_STATE_SECRET missing or empty");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "oauth state signing not configured"})),
            )
                .into_response());
        }
    };
    let mismatch_response = || {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!(
                    "the {} surface requires a freshly minted signed state for this surface — \
                     reconnect and try again",
                    surface.as_str()
                )
            })),
        )
            .into_response()
    };
    let verified =
        oauth_state::verify_state(&secret, state_val).map_err(|_| mismatch_response())?;
    let claim_surface = verified
        .surface
        .as_deref()
        .and_then(connectors::google::Surface::parse);
    if verified.user_id.is_none() || claim_surface != Some(surface) {
        tracing::info!(
            surface = surface.as_str(),
            claim_surface = ?claim_surface,
            has_uid = verified.user_id.is_some(),
            "google authorize refused: state does not declare this exact surface \
             and a connecting user id"
        );
        return Err(mismatch_response());
    }
    Ok(())
}

/// Whether `surface` (a non-[`is_grandfathered`](connectors::google::Surface::is_grandfathered)
/// Google surface — Gmail, Chat, or Meet) may be requested via
/// [`authorize`]. [`connectors::google::surface_env_default`] is the fleet-wide
/// default; a per-org `org_connector_flags` row (Foundation train PR 1.3 —
/// see [`crate::connector_flags::resolve`]) overrides it when the caller's
/// `state` token resolves to an org account.
///
/// Best-effort account/org resolution, deliberately: `authorize` has never
/// required `state` to identify a caller (it defaults to a fresh UUID when
/// absent — see its own doc), so an absent, structurally-invalid, or
/// signature-invalid `state` here just means "can't tell which org, fall
/// back to the env default" rather than a hard failure — the same posture
/// [`crate::connector_flags::resolve`] itself takes when no control pool is
/// configured. This does NOT consume the state's jti (verification only) —
/// consumption stays the single-use choke point inside [`callback`].
async fn google_surface_enabled_for(
    state: &AppState,
    oauth_state_val: &str,
    surface: connectors::google::Surface,
) -> bool {
    let env_default = connectors::google::surface_env_default(surface);

    let Some(pool) = &state.db else {
        return env_default;
    };
    let Some(secret) = oauth_state::state_secret() else {
        return env_default;
    };
    let Ok(verified) = oauth_state::verify_state(&secret, oauth_state_val) else {
        return env_default;
    };

    let row: Result<Option<Option<String>>, sqlx::Error> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(verified.account_id)
            .fetch_optional(pool)
            .await;

    match row {
        Ok(Some(Some(org_id))) => {
            crate::connector_flags::resolve(state, &org_id, surface.flag_provider(), env_default)
                .await
        }
        // No account row, or an account with no clerk_org_id (personal
        // plane) — nothing to override against; use the env default.
        Ok(_) => env_default,
        Err(e) => {
            tracing::error!(
                error = %e,
                surface = surface.as_str(),
                "google surface gate: account lookup failed; falling back to env default"
            );
            env_default
        }
    }
}

/// Redirect a tenant admin to the provider's org-wide admin-consent page.
///
/// For providers that support it (e.g. Microsoft), this authorizes CoralSwarm for
/// the whole organization in one action. The provider returns a normal `code` to
/// the standard `/v1/oauth/{provider}/callback`, so no extra callback is needed.
/// `400` if the provider has no org-wide admin-consent flow; `404` if unknown.
///
/// # `state` must be a signed token — no client-supplied raw UUID (CS-110
/// follow-up, foundation train PR 1.4)
///
/// This handler used to accept ANY client-supplied `state`, defaulting to a
/// freshly-minted raw UUID when absent — identical to [`authorize`]'s own
/// shape. That was flagged as "harmless today" only because the shared
/// `callback` already requires a cryptographically signed token
/// (`docs/plans/org-connectors-and-channel-ingest.md` §10), so a raw-UUID
/// state would fail there regardless. It stops being harmless the moment
/// this route becomes the org-install entry point (Train 3): at that point a
/// caller must not be able to kick off an admin-consent round trip — a real
/// interactive step for a tenant admin — with a state this server never
/// minted. So this handler now requires exactly what the frontend is already
/// expected to have done: call `mint_state` first (which derives the
/// account from the AUTHENTICATED session, never a client parameter — see
/// `mint_state`'s own doc) and pass the signed result here. A missing,
/// structurally-malformed, or cryptographically-invalid `state` is now a
/// `400` here, before ever redirecting to the provider — not deferred to the
/// callback. This check does NOT consume the state's jti; consumption is a
/// single-use choke point that lives only in `callback`, after the
/// provider's `code` is in hand.
pub async fn admin_consent(
    Path(provider): Path<String>,
    Query(params): Query<AuthorizeParams>,
) -> Response {
    let connector = match connectors::lookup(&provider) {
        Some(c) => c,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("unknown provider: {provider}")})),
            )
                .into_response();
        }
    };

    let state_val = match &params.state {
        Some(s) if oauth_state::looks_like_token(s) => s.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing or invalid state parameter"})),
            )
                .into_response();
        }
    };
    let secret = match oauth_state::state_secret() {
        Some(s) => s,
        None => {
            tracing::error!("OAUTH_STATE_SECRET missing or empty");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "oauth state signing not configured"})),
            )
                .into_response();
        }
    };
    if oauth_state::verify_state(&secret, &state_val).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid state parameter"})),
        )
            .into_response();
    }

    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());

    match connector.admin_consent_url(&base_url, &state_val) {
        Some(Ok(url)) => Redirect::temporary(&url).into_response(),
        Some(Err(e)) => connector_error_response(e),
        None => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("{provider} does not support org-wide admin consent")})),
        )
            .into_response(),
    }
}

/// Map a [`ConnectorError`] to the HTTP status the old hardcoded handlers used.
pub(crate) fn connector_error_response(e: ConnectorError) -> Response {
    match e {
        ConnectorError::NotConfigured(msg) => {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": msg}))).into_response()
        }
        ConnectorError::NotImplemented(msg) => {
            (StatusCode::NOT_IMPLEMENTED, Json(json!({"error": msg}))).into_response()
        }
        ConnectorError::Exchange(msg) | ConnectorError::UpstreamStatus { message: msg, .. } => {
            // Log the provider detail; return a generic 502 to the browser.
            tracing::warn!(error = %msg, "connector exchange failed");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "failed to exchange code"})),
            )
                .into_response()
        }
        ConnectorError::Internal(msg) => {
            tracing::error!(error = %msg, "connector internal failure");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
        ConnectorError::CredentialRevoked { provider, reason } => {
            // CS-453: same `401` mapping as `routes::connectors`' own
            // `connector_error_response` — see that one's doc comment for
            // the reasoning. This route's callers (code exchange) don't yet
            // have the owner/pool context to record `credential_invalid_at`
            // here (a fresh exchange establishing a NEW grant is not "this
            // credential died", so there is nothing to mark); the durable
            // write path is `store_token_bundle_tx`'s CLEAR side below, and
            // `routes::connectors`' SET side, both keyed to this SAME
            // `oauth_tokens` row.
            tracing::warn!(%provider, reason = reason.code(), "connector credential revoked");
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "credential revoked", "reason": reason.code()})),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// GET /v1/oauth/:provider/callback
// ---------------------------------------------------------------------------

/// `GET /v1/oauth/:provider/callback`: [`callback_inner`], plus the redirect to
/// `/connectors/return` for a connect started from the generic connector API.
///
/// A state without the `ret` claim (everything the older UI mints) takes the
/// first line and is answered exactly as before. A state with it runs the very
/// same handler, then its answer is turned into the return page's
/// `result=ok|error[&code=]` by [`connectors::catalog::connect::classify`]. The
/// member's `connector_sources` rows are written first, so the connector reads
/// `setting_up` and capture finds a new connection that has picked nothing.
/// Nothing in the handler itself knows about this.
pub async fn callback(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Query(params): Query<CallbackParams>,
) -> Response {
    use connectors::catalog::connect::{classify, record_connect, return_url, Outcome};

    let Some(ret) = connect_return_to(params.state.as_deref()) else {
        return callback_inner(State(state), Path(provider), Query(params)).await;
    };
    let provider_error = params.error.clone();
    let state_token = params.state.clone();

    // The member's rows are written BEFORE the handler runs, not after it. The
    // handler starts capture as it connects (Slack indexes the member's channels
    // and reads history in a task it spawns), and capture reads these rows to
    // learn that this is a new connection that has picked nothing yet; written
    // afterwards, a fast task would find no row and read the connection as one
    // from before the catalog, which takes everything. The state is verified
    // here (signature and expiry, jti not yet consumed), so the account and
    // member are the signed ones. A connect that then fails leaves rows for a
    // provider the member has no sign-in with: the connector still reads
    // `not_connected`, and the next connect finds them in place. A failure to
    // record leaves the connection to read as set up, the legacy shape.
    if let (Some(secret), Some(token), Some(adapter)) = (
        oauth_state::state_secret(),
        state_token.as_deref(),
        connectors::catalog::registry::lookup(&provider),
    ) {
        if let Some((account_id, Some(user_id))) = oauth_state::verify_state(&secret, token)
            .ok()
            .map(|v| (v.account_id, v.user_id))
        {
            if let Err(e) = record_connect(&state, account_id, user_id, adapter).await {
                tracing::warn!(
                    error = %e, %account_id, provider = %provider,
                    "connector_sources rows not recorded before a connect"
                );
            }
        }
    }
    let resp = callback_inner(State(state.clone()), Path(provider.clone()), Query(params)).await;

    let location = resp
        .headers()
        .get(axum::http::header::LOCATION)
        .and_then(|v| v.to_str().ok());
    let outcome = classify(resp.status().as_u16(), location, provider_error.as_deref());

    tracing::info!(provider = %provider, return_to = ret.wire(), ok = outcome == Outcome::Ok,
        "connector connect callback returned to the web app");

    // Success keeps the origin the old handler redirected to (it is tenant
    // aware); a failure has no redirect to read it from.
    let web_base = location
        .and_then(|l| reqwest::Url::parse(l).ok())
        .map(|u| u.origin().ascii_serialization())
        .filter(|o| o != "null")
        .unwrap_or_else(|| state.personal_web_base_url());
    match return_url(&web_base, &provider, &outcome) {
        Some(mut url) => {
            // Preserve the successful GitHub setup target through the catalog
            // return adapter. Read only the server-built successful redirect.
            if provider == "github" && outcome == Outcome::Ok {
                if let Some(id) = location
                    .and_then(|l| reqwest::Url::parse(l).ok())
                    .and_then(|u| u.query_pairs().find(|(k, _)| k == "installation_id")
                        .and_then(|(_, v)| v.parse::<i64>().ok()))
                {
                    url.push_str(&format!("&installation_id={id}"));
                }
            }
            Redirect::temporary(&url).into_response()
        },
        // Not a connector key (an unknown provider already 404ed): nothing to
        // hand back to, so the old answer stands.
        None => resp,
    }
}

/// The `ret` claim of a callback's `state`, as a [`ReturnTo`], when the state is
/// one this server signed. An expired state still names where to go back to.
fn connect_return_to(state_token: Option<&str>) -> Option<ReturnTo> {
    let secret = oauth_state::state_secret()?;
    oauth_state::return_to_claim(&secret, state_token?)
        .as_deref()
        .and_then(ReturnTo::parse)
}

/// Handle the OAuth redirect callback.
///
/// 1. Verify the signed `state` (CS-110) and extract the bound account id.
/// 2. Exchange the `code` for tokens via the provider's token endpoint.
/// 3. Derive the account DEK and encrypt the tokens.
/// 4. Upsert into `oauth_tokens`.
/// 5. Redirect to `{web_base}/connectors?connected=<provider>`, where
///    `web_base` is tenant-aware (org-slack step 5) — see
///    [`crate::AppState::web_base_url`].
///
/// This route is hit via browser redirect (no Bearer token); the account is
/// carried in the HMAC-signed `state` token the server minted from the user's
/// authenticated session (see [`mint_state`] and [`oauth_state`]).
async fn callback_inner(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Query(params): Query<CallbackParams>,
) -> Response {
    // ── Check for provider-side errors ────────────────────────────────────────
    if let Some(err) = &params.error {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("OAuth error from provider: {err}")})),
        )
            .into_response();
    }

    // ── Validate state — structural pre-check (CSRF guard, CS-110) ────────────
    // Full cryptographic verification happens after the DB-pool guard below; this
    // cheap shape check rejects a blatantly-malformed state up front (and keeps
    // the no-DB path returning 503 for a well-formed-but-unverified token).
    let state_val = match &params.state {
        Some(s) => s.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing state parameter"})),
            )
                .into_response();
        }
    };
    if !oauth_state::looks_like_token(&state_val) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid state parameter"})),
        )
            .into_response();
    }

    // ── Microsoft org-wide admin-consent redirect (m365 enterprise train PR
    // 3.2) ─────────────────────────────────────────────────────────────────
    // Distinct from the ordinary per-user consent completion this handler
    // otherwise processes below: Microsoft's `prompt=admin_consent` variant
    // of the standard `/authorize` endpoint (`admin_consent_url`) redirects
    // back here WITHOUT a `code` at all — `admin_consent=True&tenant=<AAD
    // tenant id>&scope=...&state=...` on a grant. A denial/error instead
    // comes back as `error=...` (no `admin_consent` at all, or
    // `admin_consent=True&error=...` per Microsoft's own docs) — already
    // caught by the `params.error` check above this branch, so
    // `handle_microsoft_admin_consent` only ever sees a clean grant or an
    // unrecognized `admin_consent` value. Checked before the `code`
    // requirement below so this redirect isn't rejected as "missing code".
    if provider == "microsoft" {
        if let Some(admin_consent_val) = params.admin_consent.as_deref() {
            return handle_microsoft_admin_consent(&state, &params, admin_consent_val).await;
        }
    }

    // ── Require authorisation code ────────────────────────────────────────────
    let code = match &params.code {
        Some(c) => c.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing code parameter"})),
            )
                .into_response();
        }
    };

    // ── Require DB pool ───────────────────────────────────────────────────────
    let pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };

    // ── Resolve account_id from the SIGNED state (CS-110) ─────────────────────
    // The account id is bound into an HMAC-signed token the server minted from an
    // authenticated session, so a caller cannot forge a connection onto another
    // account. A missing signing secret is a server misconfiguration (503); a bad
    // / expired / tampered token is a client error (400).
    let secret = match oauth_state::state_secret() {
        Some(s) => s,
        None => {
            tracing::error!("OAUTH_STATE_SECRET missing or empty");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "oauth state signing not configured"})),
            )
                .into_response();
        }
    };
    // `connecting_user_id`/`state_surface` (gsuite Train G2 PR 1, item 3):
    // read here, from the VERIFIED state only, and carried down to the
    // per-surface token resolution below (`resolve_google_token_target`) —
    // never re-derived from a client query param or from `bundle.scope`
    // (Google's incremental-consent union-scope hazard, see
    // `connectors::google`'s module doc §1.1).
    let (
        account_id,
        jti,
        connecting_user_id,
        state_surface,
        teams_onboarding_ticket,
        meeting_return,
    ) = match oauth_state::verify_state(&secret, &state_val) {
        Ok(v) => (
            v.account_id,
            v.jti,
            v.user_id,
            v.surface,
            v.teams_onboarding,
            v.meeting_return,
        ),
        Err(oauth_state::StateError::Expired) => {
            // Correctly signed but past its TTL (e.g. the provider's account
            // chooser was left open): send the browser back to the app with a
            // calm message instead of raw JSON. Forgery and replay keep the
            // generic 400 — they stay indistinguishable from each other.
            let web = state.personal_web_base_url();
            return Redirect::temporary(&format!("{web}/connectors?error=expired"))
                .into_response();
        }
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid state parameter"})),
            )
                .into_response();
        }
    };

    // ── Consume the state's jti — atomic replay guard (CS-110 follow-up,
    // foundation train PR 1.4) ────────────────────────────────────────────────
    // `verify_state` only proves the token is authentically signed and
    // unexpired; a signed-but-still-valid token can otherwise be replayed
    // verbatim any number of times within its whole `STATE_TTL_SECS` window.
    // `crate::oauth::jti_store::consume` closes that with a single
    // `INSERT ... ON CONFLICT (jti) DO NOTHING` — no separate check-then-
    // insert TOCTOU gap (see that module's own doc). Same generic "invalid
    // state parameter" 400 as a bad signature, deliberately, so replay and
    // forgery are indistinguishable to the caller. A store error is NOT
    // treated as "unconsumed" — it fails closed (503), matching the
    // `OAUTH_STATE_SECRET`-missing arm above.
    match crate::oauth::jti_store::consume(&pool, &jti).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                account_id = %account_id,
                provider = %provider,
                "oauth callback: state jti already consumed (replay) — rejecting"
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid state parameter"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "oauth callback: jti replay store unreachable — failing closed"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    }

    // Defence in depth: confirm the bound account still exists, AND learn
    // whether it is an org account — needed below for the org/provider gate.
    // A DB error must hard-fail (500), never collapse into "account not
    // found" (400, the pre-existing `.unwrap_or(None)` shape this replaces):
    // that would misreport a transient DB failure as a client error, and
    // (rule of thumb this PR's own review holds every cross-plane/RLS read
    // to) an `Ok(empty)` and an `Err` must never look the same.
    let account_row: Result<Option<Option<String>>, sqlx::Error> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(&pool)
            .await;
    let clerk_org_id = match account_row {
        Ok(Some(org_id)) => org_id,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid state: account not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                "failed to look up account for oauth callback"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    // ── Dispatch to the provider strategy ─────────────────────────────────────
    // Placed here (after the state/code/pool/account guards) so an unknown
    // provider still 404s but a valid request with no DB still 503s first.
    let connector = match connectors::lookup(&provider) {
        Some(c) => c,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": format!("unknown provider: {provider}")})),
            )
                .into_response();
        }
    };

    // ── Org/provider gate (org-slack step 5; Gmail arm gsuite G2 PR 6; Chat
    // arm gsuite G4 PR 6; Microsoft arm m365 PR 3.3) ──────────────────────────
    // `reject_org_oauth_management` (the gate on `mint_state`) is provider-
    // BLIND by necessity — see its own doc comment — so THIS is the one place
    // in the generic connector path that actually knows both "is this
    // account an org account" and "which provider" (and, for Google, which
    // SURFACE — from the verified state's own `sfc` claim). The full per-
    // provider/per-surface matrix lives in `google_org_callback_allowed`'s
    // own doc + unit tests: Slack on its flag, Gmail-surface Google on its
    // flag, Chat-surface Google on its OWN flag, Microsoft on the shared
    // `"microsoft"` registry flag (per-member write path built in Train
    // 3.2), everything else refused unconditionally. Checked before
    // `exchange_code` below so a doomed org+refused request never burns the
    // provider's one-time code.
    if let Some(org_id) = clerk_org_id.as_deref() {
        if !google_org_callback_allowed(&state, org_id, connector.id(), state_surface.as_deref())
            .await
        {
            tracing::info!(
                account_id = %account_id,
                provider = %connector.id(),
                org_id = %org_id,
                "oauth callback refused under an org account: only Slack, Gmail, Chat, or \
                 Microsoft — each behind its own org connector flag — may install under an org"
            );
            return (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "connector authorization is available in your personal workspace only; \
                              switch out of the organization workspace to manage it"
                })),
            )
                .into_response();
        }
        // Slack under an org additionally requires an ADMIN caller (org
        // governance decision 2026-08-04, piece 1). `mint_state` used to
        // enforce this provider-blind, but PR 3.3 removed that (Microsoft
        // member connects are deliberately self-service), which re-opened
        // the generic-flow bypass: with both org flags on, a non-admin could
        // mint a state and drive `/v1/oauth/slack/authorize` → this callback
        // to a Slack workspace WRITE with no role check anywhere (this
        // route carries no JWT, so `reject_org_slack_non_admin` can't run
        // here). Enforce it from durable state instead: the signed state's
        // `uid` against `account_members.role` — the same rows the Clerk
        // org-role claims are upserted into (migration 0081) and the same
        // `is_admin_role` definition every other admin gate shares. Fail
        // closed on a missing uid or missing membership row.
        //
        // **Skipped for a `slack_install` connect (decision 2026-10-29)**: any
        // org member may install the app as part of their own connect; see the
        // note on `reject_org_slack_non_admin`. It is the signed state's surface
        // that decides, as for `slack_member`, and only a connect started through
        // the connector API can mint it.
        //
        // **Skipped for a `slack_member` connect (CS-454, item 46a)** —
        // `state_surface` is read from the SAME verified state this block
        // already trusts for `connecting_user_id` two lines below, so this
        // is not a new trust boundary. This mints/writes ONLY the connecting
        // member's own `slack_user` row (`resolve_slack_token_target`
        // above) — never the shared workspace bot token — which is exactly
        // the class of self-service, per-member consent Gmail/Chat/Microsoft
        // are already admin-gate-free for (see this block's own precedent
        // just above for Gmail/Chat/Microsoft's shared reasoning). The admin
        // gate protects the WORKSPACE-wide asset; a member connect never
        // touches it.
        if connector.id() == "slack"
            && !matches!(
                state_surface.as_deref(),
                Some(SLACK_MEMBER_SURFACE) | Some(SLACK_INSTALL_SURFACE)
            )
        {
            // A DB error here must NOT read as "not an admin" — same
            // Ok(empty)-vs-Err rule the account lookup above enforces: an
            // infrastructure fault gets a 503 and its own log line, never an
            // authorization refusal wearing the wrong status code.
            let member_role: Option<String> = match connecting_user_id {
                Some(uid) => {
                    match sqlx::query_scalar(
                        "SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2",
                    )
                    .bind(account_id)
                    .bind(uid)
                    .fetch_optional(&pool)
                    .await
                    {
                        Ok(role) => role,
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                account_id = %account_id,
                                "org slack oauth callback: account_members role lookup FAILED — \
                                 refusing as unavailable, not as non-admin"
                            );
                            return (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(json!({"error": "service unavailable"})),
                            )
                                .into_response();
                        }
                    }
                }
                None => None,
            };
            let is_admin = member_role
                .as_deref()
                .is_some_and(crate::services::deletion::is_admin_role);
            if !is_admin {
                tracing::info!(
                    account_id = %account_id,
                    org_id = %org_id,
                    "org slack oauth callback refused: state's user is not an org admin \
                     (role check moved here from mint_state, m365 PR 3.3)"
                );
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({
                        "error": "connecting Slack for an organization requires an \
                                  organization admin"
                    })),
                )
                    .into_response();
            }
        }
    }

    // ── One-button Google Workspace connect ───────────────────────────────────
    // A verified `sfc=workspace[:...]` state writes one row per surface the
    // member granted, so it must not fall into the single-surface target
    // resolution below (which would default an unknown surface to Calendar).
    // Placed after the org/provider gate and before `exchange_code`.
    if connector.id() == "google"
        && connectors::google_workspace::is_workspace_surface(state_surface.as_deref())
    {
        return crate::routes::google_workspace_connect::handle_callback(
            &state,
            &pool,
            connector,
            account_id,
            clerk_org_id.as_deref(),
            connecting_user_id,
            state_surface.as_deref().unwrap_or_default(),
            &code,
        )
        .await;
    }

    // ── Per-surface token target (gsuite Train G2 PR 1, items 2/3; Microsoft
    // branch added m365 enterprise train PR 3.2) ──────────────────────────────
    // Resolved strictly from the VERIFIED state's own claims (`state_surface`/
    // `connecting_user_id` above) — never from a client param, never from
    // `bundle.scope` (not even fetched yet at this point). Every provider
    // other than `google`/`microsoft`, and every `google` callback whose
    // state carries no (or an unparseable) `sfc` claim — every state minted
    // before gsuite Train G2 PR 1, and every plain Calendar-flow state after
    // it — resolves to exactly the pre-PR shape: `TokenOwner::AccountWide`
    // under `connector.id()`. Checked before `exchange_code` below, same
    // "don't burn the one-time code on a doomed request" discipline as the
    // org/provider gate just above.
    let (token_owner, provider_key): (TokenOwner, &str) = if connector.id() == "google" {
        match resolve_google_token_target(
            state_surface.as_deref(),
            connecting_user_id,
            clerk_org_id.is_some(),
        ) {
            Ok(target) => target,
            Err(()) => {
                tracing::warn!(
                    account_id = %account_id,
                    "google oauth callback refused: a non-Calendar surface's state carried no \
                     connecting user id — refusing rather than silently falling back to an \
                     account-wide token (item 4's no-fallback rule)"
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid state parameter"})),
                )
                    .into_response();
            }
        }
    } else if connector.id() == "microsoft" {
        match resolve_microsoft_token_target(clerk_org_id.is_some(), connecting_user_id) {
            Ok(owner) => (owner, connector.id()),
            Err(()) => {
                // Unreachable from any real flow today: the org/provider gate
                // above already refused every non-Slack provider under an org
                // account, so `clerk_org_id.is_some()` can never be true here
                // for `microsoft` — see that gate's own comment. Defense in
                // depth for the day it lifts (Train 3.3+): a would-be
                // org-plane Member connection whose state carried no
                // connecting user id refuses rather than silently falling
                // back to an account-wide token, same no-fallback rule as
                // Google's item 4 above.
                tracing::warn!(
                    account_id = %account_id,
                    "microsoft oauth callback refused: an org-plane state carried no \
                     connecting user id — refusing rather than silently falling back to an \
                     account-wide token"
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid state parameter"})),
                )
                    .into_response();
            }
        }
    } else if connector.id() == "github" {
        // I7: the user-to-server OAuth token is ALWAYS per-member — never
        // AccountWide, never a fallback — docs/connectors/github.md §3.1.
        // Installation tokens (the credential that actually reaches repo
        // content) are minted on demand and never stored at all; this is
        // only the human-identifying token. Same no-fallback rule as
        // Google/Microsoft's own arms above: a state with no connecting
        // user id refuses rather than guessing an account-wide owner.
        match connecting_user_id {
            Some(uid) => (TokenOwner::Member(uid), connector.id()),
            None => {
                tracing::warn!(
                    account_id = %account_id,
                    "github oauth callback refused: state carried no connecting user id — \
                     refusing rather than falling back to an account-wide token"
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid state parameter"})),
                )
                    .into_response();
            }
        }
    } else if connector.id() == "slack" {
        // CS-454, item 46a: resolved strictly from the VERIFIED state's own
        // `sfc`/`uid` claims (`state_surface`/`connecting_user_id` above) —
        // same no-client-param, no-`bundle.scope` discipline every other arm
        // in this match documents for itself. A plain (no `slack_member`
        // surface) state keeps the pre-existing `AccountWide`/`"slack"`
        // shape byte-identical to before this story — see
        // `resolve_slack_token_target`'s own doc comment.
        match resolve_slack_token_target(state_surface.as_deref(), connecting_user_id) {
            Ok(target) => target,
            Err(()) => {
                tracing::warn!(
                    account_id = %account_id,
                    "slack member oauth callback refused: a slack_member-surface state carried \
                     no connecting user id — refusing rather than silently falling back to an \
                     account-wide token"
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid state parameter"})),
                )
                    .into_response();
            }
        }
    } else {
        (TokenOwner::AccountWide, connector.id())
    };

    // The callback is unauthenticated: its caller comes exclusively from
    // verified state. Recheck that caller's eligibility after consent so a
    // removed org member or another personal account's user cannot store a
    // new grant using an otherwise still-valid state.
    if provider_key == "slack_user" || is_slack_install_surface(connector.id(), state_surface.as_deref()) {
        let uid = connecting_user_id.expect("slack_user target requires a signed caller");
        let live = match clerk_org_id.as_deref() {
            Some(org) => crate::middleware::auth::live_org_membership(&pool, org, uid)
                .await
                .map(|membership| membership.is_some_and(|(id, _)| id == account_id)),
            None => sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM accounts a JOIN users u ON u.id=a.owner_user_id \
                 WHERE a.id=$1 AND a.clerk_org_id IS NULL AND a.owner_user_id=$2 AND u.deleted_at IS NULL)",
            )
            .bind(account_id)
            .bind(uid)
            .fetch_one(&pool)
            .await,
        };
        match live {
            Ok(true) => {}
            Ok(false) => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(
                        json!({"error": "Current account membership is required to connect Slack"}),
                    ),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, user_id = %uid,
                    "slack member oauth callback: live membership lookup failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        }
    }

    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());

    // ── Exchange the code for normalized tokens ───────────────────────────────
    // CS-454, item 46a: a `slack_member` connect exchanges through a
    // DIFFERENT function — `SlackConnector::exchange_code` (the trait
    // method every other branch here uses) requires a top-level bot
    // `access_token` to be present in Slack's response and 400s a response
    // without one; a user-scope-only authorize request does not necessarily
    // return one at all (see `connectors::slack::exchange_code_member`'s own
    // doc comment). Every other provider/surface is byte-identical to
    // before this story.
    //
    // Connectors redesign (B2, decision 4): a `slack_install` connect exchanges
    // once and yields TWO bundles, the workspace bot and the member's own user
    // token, stored below as the `slack` and `slack_user` rows.
    let is_slack_install = is_slack_install_surface(connector.id(), state_surface.as_deref());
    let mut slack_member_bundle: Option<TokenBundle> = None;
    let bundle = if is_slack_install {
        match connectors::slack::exchange_code_install(&state.http_client, &base_url, &code).await {
            Ok((bot, member)) => {
                slack_member_bundle = Some(member);
                bot
            }
            Err(e) => return connector_error_response(e),
        }
    } else if connector.id() == "slack"
        && state_surface.as_deref() == Some(SLACK_MEMBER_SURFACE)
    {
        match connectors::slack::exchange_code_member(&state.http_client, &base_url, &code).await {
            Ok(b) => b,
            Err(e) => return connector_error_response(e),
        }
    } else {
        match connector
            .exchange_code(&state.http_client, &base_url, &code)
            .await
        {
            Ok(b) => b,
            Err(e) => return connector_error_response(e),
        }
    };

    // The Microsoft account selected during consent must be the exact actor
    // who mentioned the bot. Check BEFORE replacing an existing member grant.
    if let Some(ticket) = teams_onboarding_ticket.as_deref() {
        let target = crate::routes::teams_onboarding::verify(&secret, ticket);
        let member_live = match (clerk_org_id.as_deref(), connecting_user_id) {
            (Some(org), Some(member)) => {
                matches!(crate::middleware::auth::live_org_membership(&pool,org,member).await,
                Ok(Some((live_account,_))) if live_account==account_id)
            }
            _ => false,
        };
        let valid = member_live
            && connector.id() == "microsoft"
            && connecting_user_id.is_some()
            && clerk_org_id.is_some()
            && matches!(target, Ok(ref target) if crate::routes::teams_onboarding::matches_identity(
                target, &bundle.provider_workspace_id, &bundle.provider_user_id));
        if !valid {
            let web = state.web_base_url(&pool, clerk_org_id.as_deref()).await;
            return Redirect::temporary(&format!("{web}/connectors/microsoft-teams?onboarding={}&onboarding_error=microsoft_identity_mismatch",
                urlencoding::encode(ticket))).into_response();
        }
    }

    if let Some(path) = meeting_return.as_deref() {
        let live = match (clerk_org_id.as_deref(), connecting_user_id) {
            (Some(org), Some(member)) => {
                matches!(crate::middleware::auth::live_org_membership(&pool,org,member).await,Ok(Some((live_account,_))) if live_account==account_id)
            }
            // Personal workspace: only the account's own live owner may connect a
            // meeting grant (the account-wide row is never another person's).
            (None, Some(member)) => sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM accounts a JOIN users u ON u.id=a.owner_user_id \
                 WHERE a.id=$1 AND a.clerk_org_id IS NULL AND a.owner_user_id=$2 AND u.deleted_at IS NULL)",
            )
            .bind(account_id)
            .bind(member)
            .fetch_one(&pool)
            .await
            .unwrap_or(false),
            _ => false,
        };
        if !live {
            return (StatusCode::FORBIDDEN,Json(json!({"error":"Current organization membership is required to connect this meeting"}))).into_response();
        }
        // A meeting setup callback cannot replace another actor's Microsoft grant.
        if connector.id() != "microsoft" || !crate::routes::teams_ingest::valid_meeting_return(path)
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Invalid meeting return context"})),
            )
                .into_response();
        }
        let parsed = reqwest::Url::parse(&format!("https://app.coralswarm.example{path}")).ok();
        if let Some(ticket) = parsed.as_ref().and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "meeting_ticket")
                .map(|(_, v)| v.into_owned())
        }) {
            let intent = crate::services::teams_meeting_capture::verify_intent(&secret, &ticket);
            if !matches!(intent,Ok(ref i) if i.tenant==bundle.provider_workspace_id && i.aad==bundle.provider_user_id)
            {
                return (
                    StatusCode::CONFLICT,
                    Json(
                        json!({"error":"Microsoft identity does not match this meeting installer"}),
                    ),
                )
                    .into_response();
            }
        }
    }
    // A person's own Slack grant creates its own routing row below; it
    // does not require or create a shared bot install. The one-workspace
    // checks still apply to both existing member grants and shared installs.

    // ── Org governance decision (2026-08-04), piece 2: one workspace per org ──
    // `connector.id() == "slack"` (made explicit by both trains — was
    // implicit): before the org/provider gate above grew Gmail (gsuite G2
    // PR 6) and Microsoft (m365 PR 3.3) arms, Slack was the ONLY provider an
    // org account could reach here, so `clerk_org_id.is_some()` alone was an
    // exact proxy for "this is Slack". No longer: `bundle.provider_workspace_
    // id` for a Gmail callback is the connecting mailbox's EMAIL, and for a
    // Microsoft callback it is the member's AAD tenant id — which MANY
    // members legitimately share (not a conflict the way two different Slack
    // workspaces on one org would be), and `find_other_org_slack_workspace`'s
    // queries are Slack-table-scoped but not id-format-aware — comparing a
    // Slack workspace_id against an email or AAD tenant id with `<>` is
    // trivially true, so an org with a genuine Slack connection would have
    // had every member's legitimate Gmail/Microsoft connect refused as a
    // false "second workspace" conflict. The NEW workspace id only exists from

    // this point on (`bundle.provider_workspace_id`, from the provider's own
    // token response), so this cannot run any earlier than here — but it
    // still runs BEFORE any row is written for it, same as every other check
    // in this handler. This is a cheap early exit for the ordinary
    // (non-racing) case ONLY — see `refuse_second_org_slack_workspace`'s own
    // doc comment for why it alone does not close a genuinely concurrent
    // connect; the definitive check is `refuse_second_org_slack_workspace_locked`,
    // run again below inside the write transaction.
    if clerk_org_id.is_some() && connector.id() == "slack" {
        if let Err(resp) =
            refuse_second_org_slack_workspace(&pool, account_id, &bundle.provider_workspace_id)
                .await
        {
            return resp;
        }
    }

    // ── Which plane holds THIS account's wrapped DEK ──────────────────────
    // Resolved BEFORE the write transaction opens, because it may have to
    // build the org's tenant pool (a network round-trip to a different
    // database) — work that has no business happening while a control-plane
    // transaction is held open. A personal account resolves to the control
    // plane in-process, exactly as before. An org account resolves to its own
    // tenant DB's `account_keys`, which is the entire fix: `oauth_tokens`
    // ROWS live on control for every account, but an org account's KEY does
    // not — see `resolve_callback_dek_plane`'s decision table and
    // `store_token_bundle_tx`'s doc comment for the 503 this closes.
    let dek_plane = match resolve_callback_dek_plane(
        &state,
        &pool,
        account_id,
        clerk_org_id.as_deref(),
    )
    .await
    {
        Ok(plane) => plane,
        Err(resp) => return resp,
    };

    // ── Generic encrypted persistence + control-plane routing index, ONE
    // atomic transaction (one path for every provider) ─────────────────────
    //
    // `oauth_tokens` (via `store_token_bundle_tx`) and `connector_installs`
    // (via `record_connector_install`) are written in the SAME scoped
    // transaction and committed together — NOT the best-effort discipline
    // `ConnectorProvider::post_store` below still uses. `connector_installs`
    // is now the SOLE table the inbound router will resolve a workspace
    // through (`resolve_slack_account_id` reads it exclusively, and
    // `ingest_slack_message` — the highest-traffic Slack call site — is
    // routed through that same resolver). A transient failure writing it
    // AFTER `oauth_tokens` already committed would leave a fully-successful-
    // looking install with no way for any future webhook to ever find it
    // again — there is no natural retry for a one-time OAuth callback, the
    // same "this is the ONLY moment that schedules the repair" reasoning
    // `routes/slack_events.rs::persist_identity_and_enqueue_backfill` already
    // documents for its own commit-together identity+backfill pair (see its
    // call site's comment there). So this install FAILS (500) rather than
    // half-succeeds if either write fails.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(&pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                provider = %connector.id(),
                "failed to open scoped tx for oauth_tokens/connector_installs upsert"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to persist token"})),
            )
                .into_response();
        }
    };

    // Org governance decision (2026-08-04), piece 2 — the definitive,
    // race-closing recheck (see `refuse_second_org_slack_workspace_locked`'s
    // own doc comment): same condition as the pre-check above — including
    // the explicit `connector.id() == "slack"` guard (made explicit by both
    // trains independently: gsuite G2 PR 6 for the Gmail false-conflict,
    // m365 PR 3.3 for Microsoft's shared-AAD-tenant-id shape; see the
    // pre-check's own comment) — run again inside THIS transaction after
    // taking a per-account advisory lock, so two concurrent connects for two
    // different workspaces can no longer both slip past the earlier

    // pool-based check.
    if clerk_org_id.is_some() && connector.id() == "slack" {
        if let Err(resp) = refuse_second_org_slack_workspace_locked(
            &mut tx,
            account_id,
            &bundle.provider_workspace_id,
        )
        .await
        {
            tx.rollback().await.ok();
            return resp;
        }
    }

    // An install-and-sign-in must not clobber a healthy bot that is already
    // there (an admin's, or an earlier member's): the member is only signing
    // themselves in. Checked inside this transaction, against the bot row for
    // this workspace, after the advisory lock above. A dead or under-scoped bot
    // is replaced, which is how a reinstall heals it.
    let keep_bot = is_slack_install
        && match sqlx::query_as::<_, (String, Option<DateTime<Utc>>)>(
            "SELECT scope, credential_invalid_at FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack' AND owner_user_id IS NULL \
               AND provider_workspace_id = $2",
        )
        .bind(account_id)
        .bind(&bundle.provider_workspace_id)
        .fetch_optional(&mut *tx)
        .await
        {
            Ok(Some((scope, invalid_at))) => {
                invalid_at.is_none()
                    && !connectors::slack::slack_missing_scopes(&scope)
                        .is_some_and(|missing| !missing.is_empty())
            }
            Ok(None) => false,
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id,
                    "slack install: could not read the existing bot row");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "failed to persist token"})),
                )
                    .into_response();
            }
        };

    // `token_owner`/`provider_key` resolved above, strictly from the
    // verified state (item 3): every provider other than a real Gmail
    // connection keeps the pre-PR `TokenOwner::AccountWide` shape under
    // `connector.id()` — see `TokenOwner`'s own doc comment and
    // `resolve_google_token_target`'s.
    if !keep_bot {
        if let Err(resp) = store_token_bundle_tx(
            &mut tx,
            &dek_plane,
            account_id,
            provider_key,
            token_owner,
            &bundle,
        )
        .await
        {
            return resp;
        }
    }
    // Written under the SAME `(provider_key, owner)` pair as the
    // `oauth_tokens` row just above (CodeRabbit round-1 fix on this PR): a
    // Calendar connect keeps the pre-PR `connector.id()`/`NULL` shape
    // (`provider_key == connector.id() == "google"` for Calendar — see
    // `resolve_google_token_target`), while a Gmail connect writes under
    // `"google_gmail"`/`Some(member_id)` — never the OLD shared `connector.
    // id()`/`NULL` shape, which would have let a Gmail connect silently
    // create-or-clobber Calendar's shared install row (or vice versa) and
    // backed a member-owned token with an owner-`NULL` "anyone in the
    // account" install row. Nothing reads `connector_installs` for provider
    // `"google"`/`"google_gmail"` today (Calendar's own push webhook uses
    // `resolve_google_watch_channel_account_id`, a dedicated bootstrap
    // resolver — see `google_calendar_push.rs`; only Slack/Microsoft read
    // `connector_installs` at all, via `resolve_connector_install_account_ids`)
    // — this is forward-hygiene for whenever that changes (G2.3's own Gmail
    // push resolver is `resolve_gmail_watch_owner`, its own dedicated
    // function, deliberately NOT this table), not a behavior fix for a
    // reachable bug today.
    //
    // **Slack is the one deliberate exception to "same literal as
    // `oauth_tokens`" (CS-454, item 46a).** `oauth_tokens.provider` must be
    // the distinct `"slack_user"` literal for a member row (see
    // `resolve_slack_token_target`'s own doc comment: that isolation is the
    // whole point). `connector_installs.provider`, though, is the
    // workspace→account ROUTING key `routes::slack::resolve_slack_account_ids`
    // reads with a hardcoded `provider = 'slack'` literal — writing
    // `"slack_user"` there would make a member's own install invisible to
    // that resolver, defeating item 3 of this story ("connector_installs
    // gains per-owner rows so workspace→account routing still works for
    // member connects"). So `connector_installs_provider` is `"slack"`
    // regardless of which `oauth_tokens` row this write mirrors — for the
    // pre-existing admin bot-install path `provider_key` is ALREADY
    // `"slack"`, so this is byte-identical there; only the new member path
    // diverges from `provider_key`.
    let connector_installs_provider: &str = if connector.id() == "slack" {
        "slack"
    } else {
        provider_key
    };
    if !keep_bot {
        if let Err(e) = record_connector_install(
        &mut tx,
        account_id,
        connector_installs_provider,
        &bundle.provider_workspace_id,
        Some(bundle.provider_user_id.as_str()),
        token_owner.owner_user_id(),
    )
    .await
    {
        tracing::error!(
            error = %e,
            account_id = %account_id,
            provider = %provider_key,
            workspace = %bundle.provider_workspace_id,
            "failed to upsert connector_installs — failing the install rather than leaving it \
             unroutable (see this call site's own comment)"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to persist connector install"})),
        )
            .into_response();
    }
    }

    // Install-and-sign-in (B2, decision 4): the member's own row from the same
    // consent, in the same transaction as the bot row above, so the connect is
    // all or nothing. `connector_installs.provider` stays `slack` for the routing
    // reasons documented above.
    if let (Some(member), Some(uid)) = (slack_member_bundle.as_ref(), connecting_user_id) {
        if let Err(resp) = store_token_bundle_tx(
            &mut tx,
            &dek_plane,
            account_id,
            "slack_user",
            TokenOwner::Member(uid),
            member,
        )
        .await
        {
            return resp;
        }
        if let Err(e) = record_connector_install(
            &mut tx,
            account_id,
            "slack",
            &member.provider_workspace_id,
            Some(member.provider_user_id.as_str()),
            Some(uid),
        )
        .await
        {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                "failed to upsert the member's connector_installs row for a slack install — \
                 failing the install rather than leaving the member unroutable"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to persist connector install"})),
            )
                .into_response();
        }
    }

    // Connector compliance audit (m365 enterprise train PR 4.2): written in
    // the SAME transaction as `oauth_tokens`/`connector_installs` above —
    // durability tier 1 (`record_in_tx`), the hard bar this table exists
    // for. `set_tenant_user` first, when a real connecting user is known,
    // so the control-plane INSERT policy's `actor_user_id = app.user_id`
    // arm is satisfiable — `tx` was opened via `begin_account_scoped` alone
    // above, which never sets `app.user_id`.
    let audit_actor = match connecting_user_id {
        Some(uid) => {
            if let Err(e) = crate::tenancy::scope::set_tenant_user(&mut tx, uid).await {
                tracing::error!(error = %e, account_id = %account_id, "failed to set app.user_id for connector_audit_log write");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
            crate::services::connector_audit::Actor::User(uid)
        }
        None => crate::services::connector_audit::Actor::System,
    };
    // One 'install' row per credential row this connect wrote, attributed to the
    // connecting member: the bot (unless a healthy one was kept) and, for an
    // install-and-sign-in, the member's own `slack_user` row.
    let mut audited: Vec<&str> = Vec::new();
    if !keep_bot {
        audited.push(provider_key);
    }
    if slack_member_bundle.is_some() {
        audited.push("slack_user");
    }
    for audited_provider in audited {
        if let Err(e) = crate::services::connector_audit::record_in_tx(
            &mut tx,
            crate::services::connector_audit::AuditEntry {
                account_id,
                actor: audit_actor,
                action: "install",
                provider: Some(audited_provider.to_string()),
                subject: Some(bundle.provider_workspace_id.clone()),
                detail: None,
            },
        )
        .await
        {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                provider = %audited_provider,
                "failed to write connector_audit_log 'install' row — failing the install rather \
                 than committing an unaudited connector connection"
            );
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to persist audit record"})),
            )
                .into_response();
        }
    }

    if let Err(e) = tx.commit().await {
        tracing::error!(
            error = %e,
            account_id = %account_id,
            provider = %connector.id(),
            "failed to commit oauth_tokens/connector_installs transaction"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to persist token"})),
        )
            .into_response();
    }

    if let Err(e)=crate::read_models::dirty::connector_account_changed(&state,account_id,connecting_user_id.unwrap_or(Uuid::nil())).await {
        tracing::error!(error=%e,"OAuth callback read-model invalidation failed");
        return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"read model invalidation failed"}))).into_response();
    }

    // ── Provider-specific side effects (best-effort; never fails the redirect) ─
    // `post_store`'s own `slack_integrations` mirror is DELIBERATELY still
    // best-effort — see its own doc comment. It is not the routing index, and
    // `ingest_slack_message`'s fallback (re-reading `slack_integrations` after
    // resolving via `connector_installs`) already treats a missing row there
    // as a recoverable "dead connector for this message", not a silent
    // permanent drop the way a missing `connector_installs` row would be.
    // `provider_key` (CodeRabbit round-1 fix): the SAME per-surface literal
    // just used for the `oauth_tokens`/`connector_installs` writes above —
    // see `ConnectorProvider::post_store`'s own doc for why this, not
    // `bundle.scope`, is what a surface-splitting provider (Google) must
    // gate on.
    //
    // CS-454 CodeRabbit: skip every `post_store` side effect for a
    // `slack_user` member callback. `SlackConnector::post_store` upserts
    // `slack_integrations.bot_token` from `bundle.access_token`; a member
    // row's token is `xoxp-…`, so running that upsert would replace the
    // workspace bot token ingest reads. The member path has no
    // `slack_integrations` mirror and no install-time welcome DM.
    // `SlackConnector::post_store` also returns early on this key so a
    // future call site cannot regress the same overwrite.
    if connector.id() == "google" {
        // Shared with the one-button Google Workspace callback so the two
        // flows cannot drift: `post_store` bootstrap (Calendar/Gmail watch,
        // Chat subscription, Meet sync-state) plus the Chat invite job.
        crate::routes::google_workspace_connect::google_surface_post_connect(
            &state,
            &pool,
            connector,
            account_id,
            &bundle,
            provider_key,
            connecting_user_id,
            clerk_org_id.as_deref(),
        )
        .await;
    } else if provider_key != "slack_user" && !keep_bot {
        connector
            .post_store(&state.http_client, &pool, account_id, &bundle, provider_key)
            .await;
    } else if provider_key == "slack_user" && connecting_user_id.is_some() {
        let uid = connecting_user_id.expect("checked above");
        spawn_slack_member_refresh(
            &state,
            &pool,
            account_id,
            uid,
            bundle.provider_workspace_id.clone(),
            bundle.access_token.clone(),
        );
    }

    // An install-and-sign-in connect also signed the member in: index their
    // channels from their own token, exactly as a member-only connect does.
    if let (Some(member), Some(uid)) = (slack_member_bundle.as_ref(), connecting_user_id) {
        spawn_slack_member_refresh(
            &state,
            &pool,
            account_id,
            uid,
            member.provider_workspace_id.clone(),
            member.access_token.clone(),
        );
    }

    if provider_key == "microsoft" {
        let app = state.clone();
        let control = pool.clone();
        let token = bundle.access_token.clone();
        let tenant_id = bundle.provider_workspace_id.clone();
        tokio::spawn(async move {
            crate::connectors::microsoft::email_unmatched_joined_teams(
                &app, &control, account_id, &token,
            )
            .await;
            // The connect-time walk above is immediate. The job is the later
            // walk (and the seat reconciler enqueues the same kind). Jobs for
            // an org land on that org's tenant pool, the plane the worker
            // dequeues from.
            let (jobs_pool, wake_org) = match crate::routes::slack::account_plane(
                &control, account_id,
            )
            .await
            {
                Ok(crate::routes::slack::AccountPlane::Personal) => (control.clone(), None),
                Ok(crate::routes::slack::AccountPlane::Org) => {
                    let clerk: Option<String> =
                        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
                            .bind(account_id)
                            .fetch_optional(&control)
                            .await
                            .ok()
                            .flatten();
                    let Some(org) = clerk else {
                        return;
                    };
                    match crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
                        &app, &control, &org,
                    )
                    .await
                    {
                        // Org jobs live on that tenant's jobs table. Wake the
                        // harness — an org in idle backoff will not see the
                        // row otherwise. The consumer plane never backs off.
                        Some(pool) => (pool, Some(org)),
                        None => return,
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e, %account_id,
                        "teams invite: could not resolve the jobs plane"
                    );
                    return;
                }
            };
            if let Some(org) = wake_org.as_deref() {
                app.worker_activity.note_enqueue(org);
            }
            crate::connectors::microsoft::enqueue_joined_team_invite_jobs(
                &app.http_client,
                &jobs_pool,
                account_id,
                &token,
                &tenant_id,
            )
            .await;
        });
    }

    // ── GitHub: record the INSTALLATION (docs/connectors/github.md §3.1) ──
    // Cannot live inside `ConnectorProvider::post_store` — that trait method
    // does not carry `installation_id`/`setup_action` (see
    // `GithubConnector`'s own `post_store` doc comment for why: every other
    // provider's post-store side effect fits inside `bundle` alone). Runs
    // AFTER the generic `oauth_tokens` write has committed (best-effort,
    // like every other `post_store` side effect) — a failure here means the
    // per-member token is stored but the installation's routing row is not
    // yet recorded; the App's Setup-URL round trip
    // (`GET /app/installations/{id}`, `connectors::github::fetch_installation`)
    // is the self-heal path `routes::github::list_repos`/`list_installs`
    // fall back to for an installation this callback never got to register.
    if connector.id() == "github" {
        if let Some(installation_id) = params
            .installation_id
            .as_deref()
            .and_then(|s| s.parse::<i64>().ok())
        {
            // `setup_action` (`"install"` | `"update"`) distinguishes a
            // fresh install from a reconfiguration round trip; both take the
            // identical record-installation path below (an "update" just
            // re-upserts the same row), so it is observability only here —
            // logged, not branched on.
            tracing::debug!(
                account_id = %account_id,
                installation_id,
                setup_action = params.setup_action.as_deref().unwrap_or(""),
                "github oauth callback: install round trip"
            );
            match crate::connectors::github::fetch_installation(&state.http_client, installation_id)
                .await
            {
                Ok(info) => {
                    match crate::connectors::github::record_installation(
                        &pool,
                        account_id,
                        installation_id,
                        &info,
                        connecting_user_id,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(crate::connectors::github::RecordInstallationError::OwnedByOtherAccount {
                            existing_account_id,
                        }) => {
                            tracing::error!(
                                requested_account_id = %account_id,
                                existing_account_id = %existing_account_id,
                                installation_id,
                                "github oauth callback: installation already owned by another \
                                 account — refusing to record a conflicting routing row"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                account_id = %account_id,
                                installation_id,
                                "github oauth callback: failed to record the installation — the \
                                 per-member token is stored, but this installation is not yet \
                                 routable from a webhook until the next successful record"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        account_id = %account_id,
                        installation_id,
                        "github oauth callback: could not fetch installation metadata from GitHub \
                         — installation not recorded this round trip"
                    );
                }
            }
        }
    }

    if let Err(e)=crate::read_models::dirty::connector_account_changed(&state,account_id,connecting_user_id.unwrap_or(Uuid::nil())).await {
        tracing::error!(error=%e,"OAuth callback read-model invalidation failed");
        return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"read model invalidation failed"}))).into_response();
    }

    tracing::info!(
        account_id = %account_id,
        provider   = %connector.id(),
        workspace  = %bundle.provider_workspace_id,
        "OAuth connection established"
    );

    // ── Redirect to frontend ──────────────────────────────────────────────────
    // Tenant-aware (org-slack step 5): a single static `FRONTEND_URL` cannot
    // be right once orgs exist — see `AppState::web_base_url`'s own doc
    // comment for the full "app.{apex} 403s an org token" reasoning.
    // `clerk_org_id` was already resolved above for the org/provider gate;
    // reused here rather than re-queried.
    let frontend_url = state.web_base_url(&pool, clerk_org_id.as_deref()).await;
    // GitHub lands on the repo SELECTION page for the installation it just
    // recorded, not the generic tile grid: §3.3 "grant, then choose" — the
    // install is not a connection until repos and destinations are picked,
    // and the selection page is where that happens. Found during the first
    // real install round trip (2026-09-21): the generic redirect below left
    // the admin on the tile grid with a "not connected" badge and a "Manage"
    // link to find on their own. Other providers keep the generic shape.
    let github_installation_id = (connector.id() == "github")
        .then(|| {
            params
                .installation_id
                .as_deref()
                .and_then(|s| s.parse::<i64>().ok())
        })
        .flatten();
    if let Some(ticket) = teams_onboarding_ticket.as_deref() {
        let result = match (
            connecting_user_id,
            crate::routes::teams_onboarding::verify(&secret, ticket),
        ) {
            (Some(member), Ok(target)) => {
                crate::routes::teams_onboarding::activate(&state, account_id, member, &target).await
            }
            _ => Err(crate::routes::teams_onboarding::Error::Identity),
        };
        let suffix = if result.is_ok() {
            ""
        } else {
            "&onboarding_error=capture_unavailable"
        };
        // Keep the signed target: the authenticated page re-verifies actual
        // persisted capture instead of trusting a browser-controlled result.
        return Redirect::temporary(&format!(
            "{frontend_url}/connectors/microsoft-teams?onboarding={}{suffix}",
            urlencoding::encode(ticket)
        ))
        .into_response();
    }
    if let Some(path) = meeting_return
        .as_deref()
        .filter(|p| crate::routes::teams_ingest::valid_meeting_return(p))
    {
        // Preserve the exact selector; the page still requires explicit consent.
        return Redirect::temporary(&format!("{frontend_url}{path}")).into_response();
    }
    let dest = match github_installation_id {
        Some(installation_id) => {
            format!("{frontend_url}/connectors/github?installation_id={installation_id}")
        }
        None => format!("{frontend_url}/connectors?connected={}", connector.id()),
    };
    Redirect::temporary(&dest).into_response()
}

/// Index a member's Slack channels from their own `xoxp-` token and pull one
/// history page (item 46b). Best-effort and detached: never fails the redirect.
fn spawn_slack_member_refresh(
    state: &Arc<AppState>,
    pool: &PgPool,
    account_id: Uuid,
    uid: Uuid,
    workspace: String,
    token: String,
) {
    // Item 46b: index this member's channels from their own xoxp token
    // and pull one history page. Best-effort — never fails the redirect.
    let http = state.http_client.clone();
    let control = pool.clone();
    let app = state.clone();
    tokio::spawn(async move {
        let plane = crate::routes::slack::account_plane(&control, account_id)
            .await
            .unwrap_or(crate::routes::slack::AccountPlane::Personal);
        let clerk: Option<String> =
            sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
                .bind(account_id)
                .fetch_optional(&control)
                .await
                .ok()
                .flatten();
        let content = match plane {
            crate::routes::slack::AccountPlane::Personal => control.clone(),
            crate::routes::slack::AccountPlane::Org => {
                let Some(org) = clerk.as_deref() else { return };
                match crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
                    &app, &control, org,
                )
                .await
                {
                    Some(p) => p,
                    None => return,
                }
            }
        };
        use crate::connectors::slack_member_capture::ConnectRefreshError;
        match crate::connectors::slack_member_capture::refresh_and_backfill_on_connect(
            &app,
            &http,
            &control,
            &content,
            account_id,
            uid,
            &workspace,
            &token,
            plane,
            clerk.as_deref(),
        )
        .await
        {
            Ok(()) => {}
            // A membership outcome (the user left between auth and this
            // spawned task), not an infrastructure fault.
            Err(ConnectRefreshError::NotMember) => tracing::info!(
                %account_id,
                user_id = %uid,
                "slack member channel index refresh skipped: not a current member of the account"
            ),
            Err(ConnectRefreshError::Failed(e)) => {
                tracing::warn!(error = %e, %account_id, "slack member channel index refresh failed")
            }
        }
    });
}

/// Resolve the `(TokenOwner, provider_key)` pair [`callback`] persists a
/// Google connection under (gsuite Train G2 PR 1, items 2/3) — pure, so it is
/// directly unit-testable without a DB. `surface_claim`/`connecting_user_id`
/// must come from the VERIFIED OAuth state only (`callback`'s own
/// `state_surface`/`connecting_user_id`), never a client query param and
/// never `bundle.scope` (the incremental-consent union-scope hazard —
/// `connectors::google`'s module doc §1.1).
///
/// An absent or unparseable `surface_claim` defaults to
/// [`Surface::Calendar`](connectors::google::Surface::Calendar) — every state
/// minted before this PR, and every plain (no `?surface=`) Calendar-flow
/// state after it, carries no `sfc` claim at all, so this keeps the
/// pre-existing Calendar callback path byte-identical.
///
/// A non-Calendar surface (Gmail today) with no `connecting_user_id`
/// returns `Err(())` — [`callback`] turns that into a `400`, never a silent
/// fallback to `TokenOwner::AccountWide` (item 4's rule). In practice this
/// is unreachable from any real flow: `authorize`'s own
/// `require_verified_surface_state` refuses to redirect to Google at all
/// unless the state already carries both a matching `sfc` and a `uid`, and
/// `mint_state` always sets `uid` alongside any `sfc` it sets. This is
/// defense in depth, not a path a genuine Gmail consent can reach.
pub(crate) fn resolve_google_token_target(
    surface_claim: Option<&str>,
    connecting_user_id: Option<Uuid>,
    is_org: bool,
) -> Result<(TokenOwner, &'static str), ()> {
    let surface = surface_claim
        .and_then(connectors::google::Surface::parse)
        .unwrap_or(connectors::google::Surface::Calendar);
    match surface {
        // Google Calendar org lift: PERSONAL plane keeps the grandfathered
        // account-wide row (byte-identical, `connecting_user_id` ignored —
        // exactly as before); an ORG account stores a per-member row under
        // the same `"google"` literal, with the same no-fallback rule as
        // `resolve_microsoft_token_target`: an org connect whose state
        // carries no connecting user id is REFUSED, never silently stored
        // account-wide (which would let one member's connect overwrite every
        // other member's calendar credential).
        connectors::google::Surface::Calendar if is_org => {
            let uid = connecting_user_id.ok_or(())?;
            Ok((
                connectors::google::Surface::calendar_token_owner(true, uid),
                surface.token_provider(),
            ))
        }
        connectors::google::Surface::Calendar => {
            Ok((TokenOwner::AccountWide, surface.token_provider()))
        }
        _ => {
            let uid = connecting_user_id.ok_or(())?;
            Ok((surface.token_owner_for(uid), surface.token_provider()))
        }
    }
}

/// Resolve the [`TokenOwner`] [`callback`] persists a Microsoft connection
/// under (m365 enterprise train PR 3.2, "delegated per-member tokens") —
/// pure, so it is directly unit-testable without a DB. `connecting_user_id`
/// must come from the VERIFIED OAuth state only (`callback`'s own
/// `connecting_user_id`), never a client query param.
///
/// Unlike Google, Microsoft has no separate OAuth "surface" per data type —
/// one code exchange yields one token bundle covering mail, calendar, and
/// Teams together (`connectors::microsoft::scopes`), so there is exactly one
/// `provider_key` (`"microsoft"`, i.e. `connector.id()`) regardless of
/// owner. The owner dimension instead follows account plane:
///
/// * **`is_org == false` (personal plane)** — `TokenOwner::AccountWide`,
///   byte-identical to every Microsoft connection this codebase has ever
///   written. Deliberately NOT switched to `Member(uid)` unconditionally the
///   way Gmail did (gsuite Train G2 PR 1): Microsoft already has a wide,
///   established set of AccountWide-only read call sites this PR does not
///   touch (`routes::graph_webhook`, `routes::workflows`,
///   `workers::graph_subscriptions`, `routes::teams_ingest`) — moving the
///   write path to `Member` unconditionally would silently orphan every one
///   of them for a freshly-connected account. `routes::connectors`'s own
///   member-scoped call sites (`trigger_sync`/`ingest_selected`) already
///   try `Member(caller)` first for exactly this reason (forward-compat
///   plumbing that costs nothing today — see `load_creds_owner_aware`'s own
///   doc), then fall back to this same `AccountWide` row.
/// * **`is_org == true` (org plane)** — `TokenOwner::Member(uid)`. In
///   practice UNREACHABLE today: `callback`'s org/provider gate (above)
///   already refuses every non-Slack provider under an org account, so this
///   function is never even called with `is_org == true` for `microsoft`
///   yet. Written correctly now so the day that gate lifts for Microsoft
///   (Train 3.3+), per-member isolation is already the behavior, not a
///   second migration of every existing row.
///
/// An org-plane call with no `connecting_user_id` returns `Err(())` —
/// [`callback`] turns that into a `400`, never a silent fallback to
/// `TokenOwner::AccountWide` (same no-fallback rule
/// [`resolve_google_token_target`] documents for its own Gmail branch).
fn resolve_microsoft_token_target(
    is_org: bool,
    connecting_user_id: Option<Uuid>,
) -> Result<TokenOwner, ()> {
    if is_org {
        connecting_user_id.map(TokenOwner::Member).ok_or(())
    } else {
        Ok(TokenOwner::AccountWide)
    }
}

/// Resolve the [`TokenOwner`] scope [`delete_connection`] must delete
/// EXACTLY (CS-452) — never the `owner_user_id IS NULL OR owner_user_id =
/// $n` fallback [`TokenOwner`]'s own doc comment says exists nowhere in this
/// codebase. Before this function existed, `delete_connection`'s `oauth_
/// tokens` DELETE used exactly that fallback shape, which meant a member
/// disconnecting a per-member provider (Gmail, Chat, org-plane Microsoft)
/// could ALSO delete a different, unrelated `AccountWide` row for the same
/// `(account_id, provider)` if one happened to exist (e.g. a legacy Personal-
/// plane Microsoft connection an org later grew per-member rows alongside) —
/// wiping a connection every other member still depends on.
///
/// Mirrors, for the DELETE side, the exact same provider/plane decision
/// [`callback`] already makes on the WRITE side
/// ([`resolve_google_token_target`]'s Gmail/Chat/Meet-are-`Member` split,
/// [`resolve_microsoft_token_target`]'s plane split, GitHub's unconditional
/// `Member` arm in `callback`'s own provider dispatch) — so a row this route
/// deletes is always scoped to the SAME owner dimension the row was written
/// under, never the other one.
///
/// `provider` is the STORED `oauth_tokens.provider` key (`"google"`,
/// `"google_gmail"`, `"google_chat"`, `"google_meet"`, `"microsoft"`,
/// `"slack"`, `"github"`, ...) — the URL path segment [`delete_connection`]
/// already has, not a client-suppliable surface/plane hint. `is_org` is
/// `tenant.org_context().is_some()`; `user_id` is always the AUTHENTICATED
/// caller's own id, never a client parameter.
///
/// Unlisted/future providers fall back to [`TokenOwner::AccountWide`] — the
/// SAME default `callback`'s own provider dispatch uses for "every provider
/// other than google/microsoft/github" (that `match`'s final arm) — so a new
/// simple connector that has never called this function before behaves
/// exactly as if the pre-Foundation-PR-1.1 unconditional-`AccountWide` shape
/// still applied to it.
fn resolve_delete_token_owner(provider: &str, is_org: bool, user_id: Uuid) -> TokenOwner {
    match provider {
        // Calendar's grandfathered literal — AccountWide on the personal
        // plane, per-member on the org plane exactly like the write path
        // (`resolve_google_token_target`'s Calendar arm,
        // `Surface::calendar_token_owner`; Google Calendar org lift).
        "google" => connectors::google::Surface::calendar_token_owner(is_org, user_id),
        // Gmail/Chat/Meet are unconditionally per-member
        // (`Surface::token_owner_for`'s own `Gmail | Chat | Meet` arm).
        "google_gmail" | "google_chat" | "google_meet" => TokenOwner::Member(user_id),
        // GitHub's user-to-server token is always per-member, never
        // AccountWide, never a fallback (docs/connectors/github.md §3.1).
        "github" => TokenOwner::Member(user_id),
        // Microsoft follows account plane, exactly like the write path
        // (`resolve_microsoft_token_target`): AccountWide on the personal
        // plane, per-member on the org plane (m365 enterprise train PR 3.2/
        // 3.3 — real and reachable via `org_microsoft_teams_enabled_for`,
        // not merely forward-compat plumbing; see that function's own doc
        // for why a stale "not yet built" framing no longer holds here).
        // `Some(user_id)` always succeeds in `resolve_microsoft_token_
        // target` regardless of `is_org` (its `Err(())` arm only fires on a
        // missing connecting user id, which this call site never has), so
        // the fallback below is unreachable defensive coding, not a real
        // second behavior.
        "microsoft" => {
            resolve_microsoft_token_target(is_org, Some(user_id)).unwrap_or(TokenOwner::AccountWide)
        }
        // A member's own Slack user token (CS-454) is always per-member.
        // `delete_connection` routes `slack_user` to
        // `delete_slack_member_connection` before this is reached; the arm
        // keeps this resolver correct if that early return ever goes.
        "slack_user" => TokenOwner::Member(user_id),
        // Slack, and any provider not yet given its own per-member branch:
        // the original, pre-Foundation-PR-1.1 AccountWide-only shape.
        _ => TokenOwner::AccountWide,
    }
}

/// Whether a verified state is a Slack install-and-sign-in connect.
fn is_slack_install_surface(connector_id: &str, surface: Option<&str>) -> bool {
    connector_id == "slack" && surface == Some(SLACK_INSTALL_SURFACE)
}

/// Resolve the `(TokenOwner, provider_key)` pair [`callback`] persists a
/// Slack connection under (CS-454, item 46a) — pure, so it is directly
/// unit-testable without a DB. `surface_claim`/`connecting_user_id` must
/// come from the VERIFIED OAuth state only (`callback`'s own `state_surface`/
/// `connecting_user_id`), never a client query param and never `bundle.scope`
/// — same discipline [`resolve_google_token_target`] documents for its own
/// Gmail branch.
///
/// A state with no `slack_member` surface claim (every state minted before
/// this story, and every plain admin bot-install connect after it) resolves
/// to `(TokenOwner::AccountWide, "slack")` — byte-identical to Slack's
/// pre-existing, only-ever shape. This is the one deliberate exception to
/// D21's "Slack has no per-member OAuth" (`TokenOwner`'s own doc comment,
/// `connectors::TokenOwner::AccountWide`'s variant doc) — CS-454 is what
/// makes that no longer true, under the DISTINCT provider literal
/// `"slack_user"`, never `"slack"` itself: a member row can therefore never
/// collide with, or be read as, the shared bot row (`uq_oauth_tokens_
/// account_wide`/`uq_oauth_tokens_per_owner`, migration `0175`, are keyed on
/// `provider`, so `"slack"` and `"slack_user"` are two disjoint keyspaces
/// even before the `owner_user_id` dimension is considered — belt and
/// suspenders with `resolve_account_wide_oauth_token`'s own hardcoded
/// `provider = "slack"` literal, `workers::huddle.rs`, which a `"slack_user"`
/// row can never match on the provider column alone).
///
/// A `slack_member`-surface state with no `connecting_user_id` returns
/// `Err(())` — [`callback`] turns that into a `400`, never a silent
/// fallback to `TokenOwner::AccountWide` (same no-fallback rule
/// [`resolve_google_token_target`]/[`resolve_microsoft_token_target`]
/// document for their own branches). In practice unreachable from any real
/// flow: `authorize`'s own Slack-member arm refuses to redirect at all
/// unless the state already carries both a matching `sfc` and a `uid`, and
/// `mint_state` always sets `uid` alongside any `sfc` it sets — defense in
/// depth, not a path a genuine member consent can reach.
fn resolve_slack_token_target(
    surface_claim: Option<&str>,
    connecting_user_id: Option<Uuid>,
) -> Result<(TokenOwner, &'static str), ()> {
    if surface_claim == Some(SLACK_MEMBER_SURFACE) {
        let uid = connecting_user_id.ok_or(())?;
        Ok((TokenOwner::Member(uid), "slack_user"))
    } else if surface_claim == Some(SLACK_INSTALL_SURFACE) {
        // The primary row of an install is the workspace bot, account-wide,
        // exactly as a plain admin install; the member's own `slack_user` row is
        // written beside it. Both need a signed caller, so no uid refuses.
        connecting_user_id.ok_or(())?;
        Ok((TokenOwner::AccountWide, "slack"))
    } else {
        Ok((TokenOwner::AccountWide, "slack"))
    }
}

/// Upsert a tenant admin's org-wide Microsoft consent grant (m365
/// enterprise train PR 3.2) into `microsoft_admin_consents` (migration
/// `0188`, control-only — see that migration's own header). One row per
/// `(account_id, microsoft_tenant_id)`: re-running admin-consent for the
/// same tenant just refreshes `consented_at`/`consented_by_user_id`/
/// `granted_scope`. `pub(crate)` so this module's own pg tests can exercise
/// it directly, mirroring `connectors::microsoft_disconnect::
/// purge_microsoft_connection`'s "test the function, not the HTTP route"
/// precedent.
pub(crate) async fn record_microsoft_admin_consent(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    microsoft_tenant_id: &str,
    consented_by_user_id: Option<Uuid>,
    granted_scope: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO microsoft_admin_consents
            (account_id, microsoft_tenant_id, consented_by_user_id, granted_scope, consented_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (account_id, microsoft_tenant_id)
        DO UPDATE SET consented_by_user_id = EXCLUDED.consented_by_user_id,
                       granted_scope = EXCLUDED.granted_scope,
                       consented_at = now()
        "#,
    )
    .bind(account_id)
    .bind(microsoft_tenant_id)
    .bind(consented_by_user_id)
    .bind(granted_scope)
    .execute(pool)
    .await?;
    Ok(())
}

/// Handle Microsoft's org-wide admin-consent redirect (m365 enterprise
/// train PR 3.2) — see [`callback`]'s own call site comment for exactly
/// which redirect shape lands here and why it is branched off before the
/// ordinary `code` exchange path. Same state-verification discipline as
/// that ordinary path: structural check, signature+expiry verification, and
/// single-use jti consumption — a tenant admin's consent grant is bound to
/// the CoralSwarm account that started the flow (`mint_state`'s
/// authenticated caller) exactly like a normal token callback is, and the
/// jti must not be replayable here either.
///
/// Does NOT gate on account plane (org vs personal): `admin_consent_url`
/// itself carries no such gate either (see that handler's own doc), and
/// `mint_state`'s org gate is the sole chokepoint today — it only lets an
/// org account mint a state at all when the org's Slack flag happens to be
/// on (provider-blind, see that gate's own doc), which is an existing,
/// orthogonal gap this PR does not close. In practice, today, only a
/// personal-plane account can reliably reach this branch.
async fn handle_microsoft_admin_consent(
    state: &AppState,
    params: &CallbackParams,
    admin_consent_val: &str,
) -> Response {
    let state_val = match &params.state {
        Some(s) => s.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "missing state parameter"})),
            )
                .into_response();
        }
    };
    if !oauth_state::looks_like_token(&state_val) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid state parameter"})),
        )
            .into_response();
    }
    let pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };
    let secret = match oauth_state::state_secret() {
        Some(s) => s,
        None => {
            tracing::error!("OAUTH_STATE_SECRET missing or empty");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "oauth state signing not configured"})),
            )
                .into_response();
        }
    };
    let (account_id, jti, consenting_user_id) = match oauth_state::verify_state(&secret, &state_val)
    {
        Ok(v) => (v.account_id, v.jti, v.user_id),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid state parameter"})),
            )
                .into_response();
        }
    };

    // Same atomic, fail-closed replay guard as the ordinary callback path
    // above — see that call site's own comment.
    match crate::oauth::jti_store::consume(&pool, &jti).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                account_id = %account_id,
                "microsoft admin-consent callback: state jti already consumed (replay) — rejecting"
            );
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid state parameter"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                "microsoft admin-consent callback: jti replay store unreachable — failing closed"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    }

    // Tenant-aware redirect destination, same lookup `callback` itself does
    // — best-effort: a lookup failure degrades to the default (non-org)
    // frontend URL rather than failing the whole response, since this only
    // picks a redirect target, not a security-relevant decision.
    let account_row: Result<Option<Option<String>>, sqlx::Error> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(&pool)
            .await;
    let clerk_org_id: Option<String> = account_row.ok().flatten().flatten();
    let frontend_url = state.web_base_url(&pool, clerk_org_id.as_deref()).await;

    if !admin_consent_val.eq_ignore_ascii_case("true") {
        // A denial (`admin_consent=False`) or any other unrecognized value —
        // not a hard error: the admin simply declined, or Microsoft sent a
        // shape we don't recognize. Nothing is written; redirect back with
        // an indicator the frontend can surface.
        tracing::info!(
            account_id = %account_id,
            admin_consent = %admin_consent_val,
            "microsoft admin-consent not granted — recording nothing"
        );
        let dest = format!("{frontend_url}/connectors?connected=microsoft&admin_consent=denied");
        return Redirect::temporary(&dest).into_response();
    }

    let Some(ms_tenant_id) = params.tenant.as_deref().filter(|t| !t.is_empty()) else {
        tracing::warn!(
            account_id = %account_id,
            "microsoft admin-consent: admin_consent=True but no tenant id in the redirect"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing tenant parameter"})),
        )
            .into_response();
    };

    if let Err(e) = record_microsoft_admin_consent(
        &pool,
        account_id,
        ms_tenant_id,
        consenting_user_id,
        params.scope.as_deref(),
    )
    .await
    {
        tracing::error!(
            error = %e,
            account_id = %account_id,
            tenant = %ms_tenant_id,
            "failed to record microsoft admin consent"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to persist admin consent"})),
        )
            .into_response();
    }

    tracing::info!(
        account_id = %account_id,
        tenant = %ms_tenant_id,
        "microsoft tenant-wide admin consent recorded"
    );

    // Connector compliance audit (m365 enterprise train PR 4.2, plan
    // decision #11 part 1): the HARD durability bar — `admin_consent` just
    // committed above, so a caller who cannot also record ITS audit row
    // must not be told this succeeded (see `services::connector_audit`'s
    // module doc, durability tier 2). `consenting_user_id` is `None` only
    // for a pre-item-3 state token (see `mint_state`'s own doc comment);
    // that residual case is recorded as a system actor rather than refused.
    let audit_actor = match consenting_user_id {
        Some(uid) => crate::services::connector_audit::Actor::User(uid),
        None => crate::services::connector_audit::Actor::System,
    };
    if let Err(e) = crate::services::connector_audit::record(
        &pool,
        crate::services::connector_audit::AuditEntry {
            account_id,
            actor: audit_actor,
            action: "consent",
            provider: Some("microsoft".to_string()),
            subject: Some(ms_tenant_id.to_string()),
            detail: params.scope.as_deref().map(|s| json!({"scope": s})),
        },
    )
    .await
    {
        tracing::error!(
            error = %e,
            account_id = %account_id,
            tenant = %ms_tenant_id,
            "microsoft admin-consent recorded but its connector_audit_log row failed to write \
             — failing the response per the compliance durability bar"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to persist audit record"})),
        )
            .into_response();
    }

    let dest = format!("{frontend_url}/connectors?connected=microsoft&admin_consent=granted");
    Redirect::temporary(&dest).into_response()
}

// ---------------------------------------------------------------------------
// Generic token persistence — shared by the callback and the sync-refresh path
// ---------------------------------------------------------------------------

/// Resolve which plane `account_id`'s wrapped DEK lives on, for the OAuth
/// CALLBACK — an unauthenticated browser redirect the binding gate never
/// sees, so there is no `TenancyDecision` to read a plane off (that is why
/// this cannot just be [`crate::crypto::DekPlane::from_decision`]).
///
/// **A thin mapper, not a decision table.** The table itself lives once, in
/// [`crate::crypto::DekPlane::resolve_for_org`] — shared verbatim with the
/// Slack bot-token path (`workers::huddle::resolve_account_wide_oauth_token`),
/// which is the same "signature-authenticated webhook, no session" shape and
/// must reach the identical answer. Two copies would drift, and a drift in
/// "which plane holds this org's DEK" is a wrong-key write.
///
/// This adds only the HTTP mapping: every [`crate::crypto::DekPlaneError`] is
/// an infrastructure/wiring condition, so all of them become the same 503 the
/// caller already returns for a missing key — deliberately indistinguishable
/// to the browser, and distinguished only in the log line.
pub(crate) async fn resolve_callback_dek_plane(
    state: &AppState,
    control_pool: &sqlx::PgPool,
    account_id: Uuid,
    clerk_org_id: Option<&str>,
) -> Result<DekPlane, Response> {
    DekPlane::resolve_for_org(state, control_pool, account_id, clerk_org_id)
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                org_id = ?clerk_org_id,
                connector_health = true,
                "oauth callback: refusing to resolve a DEK plane — failing closed rather than \
                 encrypting under a possibly-wrong plane's key"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "encryption key not configured"})),
            )
                .into_response()
        })
}

/// Derive the account DEK, encrypt the bundle's tokens, and upsert the row in
/// `oauth_tokens`. Provider-agnostic: every connector's tokens land here the same
/// way. On failure returns the [`Response`] the caller should return verbatim
/// (`503` if the KEK is missing, `500` on encryption/DB error).
///
/// RLS scoping (org-tenancy conversion): `oauth_tokens` carries an
/// `account_id` policy (migration 0083), so the upsert runs inside a
/// `begin_account_scoped` transaction opened here — every caller (the
/// connector sync/refresh persistence paths, the workflow and Teams
/// token-refresh helpers — see this function's own callers) inherits the
/// scope through this one funnel. Inert pre-OPS-4, load-bearing after.
///
/// **`pool` and `dek_plane` are deliberately separate.** `pool` owns the
/// `oauth_tokens` ROW and is control-plane for every account; `dek_plane` owns
/// the KEY, which for an ORG account lives in a different physical database
/// (its tenant DB's `account_keys`). This function used to take only the
/// bare `pool` and pin the plane to `DekPlane::consumer`, which made an org
/// account's token permanently un-refreshable: the write either produced a
/// row nothing could decrypt, or — after the first half of this fix — a hard
/// 503, killing the connection at its first refresh. Callers now name the
/// plane from what they already hold; see [`crate::routes::connectors::
/// load_creds_bg`]'s doc for the same three constructors on the READ side,
/// which every caller of this pairs with.
///
/// Thin wrapper over [`store_token_bundle_tx`]: opens its own transaction and
/// commits alone, because every caller of THIS signature is a token
/// **refresh** (same account/provider/workspace as an already-recorded
/// install — nothing about `connector_installs` changes), not a new install.
/// [`callback`] below, which IS a new install, calls
/// [`store_token_bundle_tx`] directly instead so it can commit the
/// `connector_installs` write in the same transaction — see that call
/// site's comment for why that atomicity is load-bearing there and not here.
/// Returns the row's new `updated_at` (see [`store_token_bundle_tx`]).
pub(crate) async fn store_token_bundle(
    pool: &sqlx::PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
    provider: &str,
    owner: TokenOwner,
    bundle: &TokenBundle,
) -> Result<DateTime<Utc>, Response> {
    let mut tx = match crate::tenancy::scope::begin_account_scoped(pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                provider = %provider,
                "failed to open scoped tx for oauth_tokens upsert"
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to persist token"})),
            )
                .into_response());
        }
    };
    let updated_at = store_token_bundle_tx(&mut tx, dek_plane, account_id, provider, owner, bundle).await?;
    if let Err(e) = tx.commit().await {
        tracing::error!(
            error = %e,
            account_id = %account_id,
            provider = %provider,
            "failed to commit oauth_tokens upsert"
        );
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to persist token"})),
        )
            .into_response());
    }
    Ok(updated_at)
}

/// Same encryption + upsert as [`store_token_bundle`], but against an
/// ALREADY-OPEN scoped transaction that this function does NOT commit — the
/// caller commits once, after every write that must be atomic with this one
/// has also succeeded. Exists so [`callback`] can write `oauth_tokens` and
/// `connector_installs` (via [`record_connector_install`]) as one atomic
/// unit; see that call site's comment.
///
/// `dek_plane` is used ONLY to resolve the account's wrapped DEK — a plain
/// read, not part of the atomic write `tx` performs, so it does not need to be
/// the same connection (or even the same DATABASE) as `tx`.
///
/// **It was a bare `pool` until the org-plane DEK fix, and that was the bug.**
/// `oauth_tokens` itself is a control-plane table for every account, so every
/// caller passed the control pool and this function called
/// `crypto::resolve_dek` — which hardcodes `TenantId::Consumer` and therefore
/// reads `accounts.dek_wrapped`. An ORG account has no `accounts.dek_wrapped`
/// at all (the shadow-provisioning fence in `clerk_provision::
/// ensure_org_account` deliberately skips it under `MULTITENANT_ENABLED`); its
/// DEK lives in its own tenant DB's `account_keys`. So every org connector
/// install fell into the `Ok(None)` arm below and returned 503 "encryption key
/// not configured" — reproduced in prod on 2026-09-06 for a Slack install
/// under an org account. The plane is now an explicit parameter
/// ([`crate::crypto::DekPlane`]) that the caller resolves, so the row's
/// storage plane (control) and its KEY's plane (control for personal, tenant
/// for org) are allowed to differ, which is exactly the real topology.
///
/// Returns the row's monotonic `updated_at` from the database write clock.
/// Transaction-start timestamps let an old reconnect transaction overwrite
/// a newer pass witness. Each update must therefore exceed the previous
/// timestamp, even when the database clock repeats or moves backward.
/// A background pass keeps this witness on
/// `ConnectorCredentials::row_updated_at` so a later `CredentialRevoked` it
/// raises is recorded only if nothing — a reconnect — wrote the row after
/// the pass last did (`credential_health::record_if_revoked_since`).
pub(crate) async fn store_token_bundle_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
    provider: &str,
    owner: TokenOwner,
    bundle: &TokenBundle,
) -> Result<DateTime<Utc>, Response> {
    let dek = dek_plane
        .resolve(account_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, account_id = %account_id, dek_plane = ?dek_plane, "failed to resolve account DEK for oauth_tokens upsert");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "encryption key not configured"})),
            )
                .into_response()
        })?
        .ok_or_else(|| {
            tracing::error!(account_id = %account_id, dek_plane = ?dek_plane, "account has no dek_wrapped provisioned");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "encryption key not configured"})),
            )
                .into_response()
        })?;

    let encrypt = |label: &'static str, plaintext: &str| -> Result<Vec<u8>, Response> {
        encrypt_token(&dek, plaintext).map_err(|e| {
            tracing::error!(error = %e, account_id = %account_id, "{label} encryption failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        })
    };

    let encrypted_access = encrypt("access_token", &bundle.access_token)?;
    let encrypted_refresh: Option<Vec<u8>> = match bundle.refresh_token.as_deref() {
        Some(rt) => Some(encrypt("refresh_token", rt)?),
        None => None,
    };
    let encrypted_user_token: Option<Vec<u8>> = match bundle.user_token.as_deref() {
        Some(ut) => Some(encrypt("user_token", ut)?),
        None => None,
    };

    // Foundation PR 1.1: `oauth_tokens` now carries two disjoint partial
    // unique indexes instead of one 3-column constraint (see
    // `connectors::TokenOwner`'s own doc comment and migration
    // `0175_oauth_tokens_owner_user_id`) — an `ON CONFLICT` target must
    // restate the exact column list AND predicate of the index it means to
    // hit, so the two branches below are genuinely different statements, not
    // a shared query with an interpolated owner value.
    let upsert_result = match owner {
        TokenOwner::AccountWide => {
            sqlx::query_scalar::<_, DateTime<Utc>>(
                r#"
                INSERT INTO oauth_tokens
                    (account_id, provider, scope, access_token, refresh_token,
                     user_token_encrypted, provider_user_id, provider_workspace_id,
                     owner_user_id, expires_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9, clock_timestamp())
                ON CONFLICT (account_id, provider, provider_workspace_id)
                    WHERE owner_user_id IS NULL
                DO UPDATE SET
                    access_token             = EXCLUDED.access_token,
                    refresh_token            = EXCLUDED.refresh_token,
                    user_token_encrypted     = EXCLUDED.user_token_encrypted,
                    scope                    = EXCLUDED.scope,
                    provider_user_id         = EXCLUDED.provider_user_id,
                    expires_at               = EXCLUDED.expires_at,
                    updated_at               = GREATEST(clock_timestamp(), oauth_tokens.updated_at + interval '1 microsecond'),
                    -- CS-453: a successful refresh/reconnect through THIS
                    -- write path is itself the "the credential is live
                    -- again" signal — clear any previously-recorded
                    -- `CredentialRevoked` marker for this exact row rather
                    -- than requiring a separate write. See
                    -- `connectors::credential_health`'s own module doc for
                    -- why this lives here instead of a dedicated
                    -- `clear_credential_invalid` call at every reconnect
                    -- call site.
                    credential_invalid_at   = NULL,
                    credential_invalid_reason = NULL
                RETURNING updated_at
                "#,
            )
            .bind(account_id)
            .bind(provider)
            .bind(&bundle.scope)
            .bind(&encrypted_access)
            .bind(&encrypted_refresh)
            .bind(&encrypted_user_token)
            .bind(&bundle.provider_user_id)
            .bind(&bundle.provider_workspace_id)
            .bind(bundle.expires_at)
            .fetch_one(&mut **tx)
            .await
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query_scalar::<_, DateTime<Utc>>(
                r#"
                INSERT INTO oauth_tokens
                    (account_id, provider, scope, access_token, refresh_token,
                     user_token_encrypted, provider_user_id, provider_workspace_id,
                     owner_user_id, expires_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, clock_timestamp())
                ON CONFLICT (account_id, owner_user_id, provider, provider_workspace_id)
                    WHERE owner_user_id IS NOT NULL
                DO UPDATE SET
                    access_token             = EXCLUDED.access_token,
                    refresh_token            = EXCLUDED.refresh_token,
                    user_token_encrypted     = EXCLUDED.user_token_encrypted,
                    scope                    = EXCLUDED.scope,
                    provider_user_id         = EXCLUDED.provider_user_id,
                    expires_at               = EXCLUDED.expires_at,
                    updated_at               = GREATEST(clock_timestamp(), oauth_tokens.updated_at + interval '1 microsecond'),
                    -- CS-453: same clear-on-success as the AccountWide arm
                    -- above — see its comment.
                    credential_invalid_at   = NULL,
                    credential_invalid_reason = NULL
                RETURNING updated_at
                "#,
            )
            .bind(account_id)
            .bind(provider)
            .bind(&bundle.scope)
            .bind(&encrypted_access)
            .bind(&encrypted_refresh)
            .bind(&encrypted_user_token)
            .bind(&bundle.provider_user_id)
            .bind(&bundle.provider_workspace_id)
            .bind(owner_user_id)
            .bind(bundle.expires_at)
            .fetch_one(&mut **tx)
            .await
        }
    };

    match upsert_result {
        Ok(updated_at) => Ok(updated_at),
        Err(e) => {
            tracing::error!(
                error = %e,
                account_id = %account_id,
                provider = %provider,
                "failed to upsert oauth_tokens"
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "failed to persist token"})),
            )
                .into_response())
        }
    }
}

// ---------------------------------------------------------------------------
// Control-plane routing index — shared by every connector OAuth path
// ---------------------------------------------------------------------------

/// Upsert the routing tuple this account's install belongs in
/// (`connector_installs`, migration `0121_connector_installs`) — the
/// control-plane index the inbound-webhook router (next step, not this PR)
/// will use to resolve a bare `(provider, workspace_id)` to every account
/// that installed it, BEFORE it can open any tenant pool.
///
/// Called from [`callback`] (inside the SAME transaction as
/// [`store_token_bundle_tx`]'s `oauth_tokens` upsert — every provider that
/// goes through the generic path) and from the legacy
/// `routes/slack.rs::oauth_callback` (inside the same transaction as its
/// `slack_integrations` upsert — that path never touches `oauth_tokens` at
/// all, see that function's own call site for why it still needs this).
///
/// **Transactional, NOT best-effort** — takes an already-open transaction
/// and does not commit it; the caller commits once, after this and the row
/// it mirrors both succeed, so an install can never half-land (`oauth_tokens`
/// or `slack_integrations` committed with no matching `connector_installs`
/// row). This was best-effort in an earlier revision of this migration, on
/// the reasoning that a missing row only degrades a not-yet-built router.
/// That reasoning stopped holding the moment `resolve_slack_account_id`
/// switched to reading `connector_installs` EXCLUSIVELY and
/// `ingest_slack_message` (the highest-traffic Slack call site) was routed
/// through it: a transient failure here, after the mirrored row already
/// committed, would look like a fully successful install to the user while
/// silently dropping every subsequent message from that workspace forever —
/// there is no natural retry for a one-time OAuth callback (same reasoning
/// `routes/slack_events.rs::persist_identity_and_enqueue_backfill` documents
/// for its own commit-together pair). The `connector_health` warning on a
/// resolve-miss (`routes/slack.rs::resolve_slack_account_id`) stays as
/// defense in depth for drift this function's own atomicity does not
/// cover (e.g. a row deleted or corrupted out from under it later), not as
/// a substitute for failing the install up front.
///
/// `owner_user_id` (gsuite Train G2 PR 1, CodeRabbit round-1 fix): `None` for
/// every existing write path (Slack's bot token and Microsoft's tenant
/// install are account-wide by design, not by omission — see migration
/// `0121`'s own header) — byte-identical to before this parameter existed,
/// when it was hardcoded `NULL` unconditionally. `Some(user_id)` for a
/// per-member connection (Gmail, `routes::oauth::callback`, passing
/// `token_owner.owner_user_id()`) — the FIRST write path to use the
/// `owner_user_id` column/`uq_connector_installs_per_owner` index the
/// migration deliberately pre-built for this day ("created now so that
/// dimension does not need its own migration later"). Mirrors this
/// row's own `oauth_tokens` counterpart exactly — a Gmail token stored as
/// `TokenOwner::Member(uid)` under `provider = "google_gmail"` now gets a
/// `connector_installs` row under the SAME `(provider, owner_user_id)` pair,
/// so the two tables can never describe different owners for the same
/// credential (the CodeRabbit-flagged hazard: a member-owned token backed by
/// an owner-`NULL`, i.e. "anyone in the account", install row). `external_user_id`
/// is normalized: an empty string (some providers' `TokenBundle::
/// provider_user_id` is `unwrap_or_default()`) is treated the same as
/// absent, so a missing claim never gets stored as if it meant something.
pub(crate) async fn record_connector_install(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    provider: &str,
    external_workspace_id: &str,
    external_user_id: Option<&str>,
    owner_user_id: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    // A failed lookup here is NOT fatal to the install: `clerk_org_id` is an
    // informational enrichment column (§9c's routing key is
    // `provider`/`external_workspace_id`/`account_id`, none of which this
    // touches), unlike the INSERT below.
    let clerk_org_id: Option<String> = sqlx::query_scalar::<_, Option<String>>(
        "SELECT clerk_org_id FROM accounts WHERE id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(
            error = %e,
            account_id = %account_id,
            provider = %provider,
            "failed to look up accounts.clerk_org_id for connector_installs write (non-fatal, \
             clerk_org_id will be recorded NULL)"
        );
        None
    })
    .flatten();

    let external_user_id = external_user_id.filter(|s| !s.is_empty());

    // Two disjoint partial unique indexes (migration 0121, same shape as
    // `oauth_tokens`' own `uq_oauth_tokens_account_wide`/`uq_oauth_tokens_
    // per_owner` split) — an `ON CONFLICT` target must restate the exact
    // column list AND predicate of the index it means to hit, so these are
    // genuinely different statements, not a shared query with an
    // interpolated owner value. Same discipline `store_token_bundle_tx`
    // already applies to `oauth_tokens`.
    match owner_user_id {
        None => {
            sqlx::query(
                r#"
                INSERT INTO connector_installs
                    (provider, external_workspace_id, external_user_id, clerk_org_id, account_id, owner_user_id)
                VALUES ($1, $2, $3, $4, $5, NULL)
                ON CONFLICT (provider, external_workspace_id, account_id) WHERE owner_user_id IS NULL
                DO UPDATE SET
                    external_user_id = EXCLUDED.external_user_id,
                    clerk_org_id      = EXCLUDED.clerk_org_id
                "#,
            )
            .bind(provider)
            .bind(external_workspace_id)
            .bind(external_user_id)
            .bind(&clerk_org_id)
            .bind(account_id)
            .execute(&mut **tx)
            .await?;
        }
        Some(owner_user_id) => {
            sqlx::query(
                r#"
                INSERT INTO connector_installs
                    (provider, external_workspace_id, external_user_id, clerk_org_id, account_id, owner_user_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (provider, external_workspace_id, account_id, owner_user_id) WHERE owner_user_id IS NOT NULL
                DO UPDATE SET
                    external_user_id = EXCLUDED.external_user_id,
                    clerk_org_id      = EXCLUDED.clerk_org_id
                "#,
            )
            .bind(provider)
            .bind(external_workspace_id)
            .bind(external_user_id)
            .bind(&clerk_org_id)
            .bind(account_id)
            .bind(owner_user_id)
            .execute(&mut **tx)
            .await?;
        }
    }

    tracing::debug!(
        account_id = %account_id,
        provider = %provider,
        workspace = %external_workspace_id,
        owner_user_id = ?owner_user_id,
        "connector_installs upserted"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// GET /v1/oauth/state  (authenticated — mint a signed state token)
// ---------------------------------------------------------------------------

/// Mint a short-lived, HMAC-signed OAuth `state` token (CS-110) bound to the
/// **authenticated** caller's account. The frontend fetches this before starting
/// a connect/admin-consent flow and passes it as `?state=`, so the account is
/// established server-side from the session — never from a client parameter.
///
/// `?surface=` (gsuite Train G2 PR 1, item 3): optionally embeds the
/// requested Google surface (e.g. `gmail`) as the state's `sfc` claim,
/// alongside the AUTHENTICATED caller's own `user_ctx.user_id` as its `uid`
/// claim — both read server-side, never trusted from anywhere else. Every
/// existing caller (no `?surface=`) still gets a `uid`-bearing but
/// `sfc`-less token, which `routes::oauth::callback` treats identically to a
/// pre-PR token: `(None, Calendar)`. An unrecognized `?surface=` value is
/// silently dropped (embeds no `sfc` claim) rather than rejected — this
/// route is provider-blind by design (see `reject_org_oauth_management`'s
/// own doc comment), so validating it against `connectors::google::Surface`
/// specifically would be a layering violation; `authorize`/`callback`
/// perform the real validation once the provider is known.
///
/// **Gmail org gate (gsuite Train G2 PR 6) / Chat org gate (gsuite Train G4
/// PR 6, "G4.6"):** `?surface=` IS read here, one step earlier than the rest
/// of this doc comment implies, purely to pick WHICH org gate applies —
/// [`reject_org_gmail_management`] or [`reject_org_chat_management`] instead
/// of [`reject_org_oauth_management`] when it parses to
/// [`connectors::google::Surface::Gmail`] or
/// [`connectors::google::Surface::Chat`] respectively. This is not the
/// layering violation the paragraph above warns against: no
/// `connectors::google::Surface` VALIDATION happens here (an unparseable
/// value still falls through to the Slack-only gate, byte-identical to
/// before either PR), only a gate SELECTION. `callback` still performs the
/// real per-surface resolution/validation once the provider is known. Gmail
/// and Chat additionally skip [`reject_org_slack_non_admin`] below — see
/// that call site's own comment.
///
/// `503` when `OAUTH_STATE_SECRET` is not configured.
pub async fn mint_state(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Query(params): Query<MintStateParams>,
) -> Response {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    match mint_state_token(&state, &tenant, &user_ctx, params.surface.as_deref(), None).await {
        Ok(token) => (
            StatusCode::OK,
            Json(json!({ "state": token, "expires_in": oauth_state::STATE_TTL_SECS })),
        )
            .into_response(),
        Err(resp) => resp,
    }
}

/// [`mint_state`]'s whole body, callable without the HTTP layer so the generic
/// connector API (`connectors::catalog::connect`) mints the very same state under
/// the very same gates. `requested_surface` is `?surface=`; `return_to` is the
/// `ret` claim of a connect started from that API (connectors redesign, B2) and
/// `None` for the older UI, whose states come out byte-identical to before.
///
/// The caller owns the interactive-session refusal; both callers apply it.
pub(crate) async fn mint_state_token(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user_ctx: &UserContext,
    requested_surface: Option<&str>,
    return_to: Option<ReturnTo>,
) -> Result<String, Response> {
    // Parsed early (gsuite Train G2 PR 6 / G4.6) so the org gate below can
    // pick the Gmail- or Chat-specific check — see this fn's own doc
    // comment.
    let surface = requested_surface.and_then(connectors::google::Surface::parse);
    let is_gmail_request = surface == Some(connectors::google::Surface::Gmail);
    let is_chat_request = surface == Some(connectors::google::Surface::Chat);
    let is_meet_request = surface == Some(connectors::google::Surface::Meet);
    // Google Calendar org lift: an EXPLICIT `?surface=calendar` mint. A
    // surface-less mint is still the legacy provider-blind path below
    // (unchanged for every personal caller); the web connect flows pass
    // `surface=calendar` so an org member's mint is gated on the Calendar flag
    // specifically instead of an unrelated provider's.
    let is_calendar_request = surface == Some(connectors::google::Surface::Calendar);
    // CS-454, item 46a: a Slack MEMBER (user-scope-only) connect. Checked
    // against the RAW param, not through `connectors::google::Surface::
    // parse` (which only recognizes Google's own surface literals and would
    // silently drop this one) — same "provider-blind param, provider-aware
    // interpretation one layer down" shape `mint_state`'s own doc comment
    // already describes for Gmail/Chat above.
    let is_slack_member_request = requested_surface == Some(SLACK_MEMBER_SURFACE);
    let is_slack_install_request = requested_surface == Some(SLACK_INSTALL_SURFACE);
    // One-button Google Workspace connect: the requested surface set is
    // resolved here, from this workspace's own flags, and signed into the
    // state's `sfc` claim (`workspace:calendar,gmail,...`).
    let mut workspace_claim: Option<String> = None;

    if connectors::google_workspace::is_workspace_surface(requested_surface) {
        match crate::routes::google_workspace_connect::mint_claim(state, tenant).await {
            Ok(claim) => workspace_claim = Some(claim),
            Err(reject) => return Err(reject),
        }
        // No `reject_org_slack_non_admin`: per-member consent, like each
        // surface it unions.
    } else if is_gmail_request {
        if let Some(reject) = reject_org_gmail_management(state, tenant).await {
            return Err(reject);
        }
        // No `reject_org_slack_non_admin` for Gmail (gsuite Train G2 PR 6):
        // Gmail is the connecting member's OWN per-member consent
        // (`TokenOwner::Member`), not an org-wide admin action the way a
        // shared Slack workspace bot token install is — see
        // `reject_org_gmail_management`'s own doc comment.
    } else if is_chat_request {
        if let Some(reject) = reject_org_chat_management(state, tenant).await {
            return Err(reject);
        }
        // No `reject_org_slack_non_admin` for Chat either (gsuite Train G4
        // PR 6): same reasoning as Gmail's arm immediately above — Chat is
        // ALSO the connecting member's own per-member consent
        // (`Surface::Chat::token_owner_for` is always `Member`), not an
        // org-wide admin action. See `reject_org_chat_management`'s own doc
        // comment for the admin check that DOES apply one layer further in
        // (routing a watched space into a team ocean), which is unrelated to
        // whether this member may connect Chat at all.
    } else if is_meet_request {
        if let Some(reject) = reject_org_meet_management(state, tenant).await {
            return Err(reject);
        }
        // No `reject_org_slack_non_admin` for Meet either: same reasoning as
        // Gmail's and Chat's arms above — Meet is the connecting member's
        // OWN per-member consent (`Surface::Meet::token_owner_for` is always
        // `Member`), not an org-wide admin action. See
        // `reject_org_meet_management`'s own doc comment.
    } else if is_calendar_request {
        if let Some(reject) = reject_org_calendar_management(state, tenant).await {
            return Err(reject);
        }
        // No `reject_org_slack_non_admin` for Calendar either: the
        // connecting member's OWN per-member consent under an org
        // (`Surface::calendar_token_owner`). Personal callers are untouched —
        // `reject_org_calendar_management` is a no-op without an org context.
    } else if is_slack_install_request {
        // Bot plus member in one consent, for any org member. Individual
        // connections only, except that Slack needs the workspace app installed to
        // deliver events at all, so the member connecting is the one who installs
        // it (decision 2026-10-29, overriding the 2026-08-04 admin-only rule for
        // this surface). The org must still have Slack enabled; Slack's own
        // app-approval settings still apply on Slack's side. The older admin
        // bot-install path (no surface) keeps `reject_org_slack_non_admin`.
        if let Some(reject) = reject_org_oauth_management(state, tenant).await {
            return Err(reject);
        }
    } else if is_slack_member_request {
        // CS-454: still runs the provider-blind flag gate (an org must have
        // Slack enabled at all — `org_slack_oauth_enabled_for`, the SAME
        // registry entry the admin bot-install flow reads, since D21 does
        // not add a second Slack flag), but deliberately NOT `reject_org_
        // slack_non_admin` — that is the whole point of item 46a's "lift the
        // admin gate for this user-scope-only connect": this mints a state
        // for the CONNECTING MEMBER's own `xoxp-…` token, never the shared
        // workspace bot token, so there is no org-wide asset being
        // authorized here for an admin-only rule to protect. `callback`'s
        // own admin check (below in this file) is skipped under the
        // identical condition — see that call site's own comment — so this
        // is not a gap opened here and left uncovered there.
        if let Some(reject) = reject_org_oauth_management(state, tenant).await {
            return Err(reject);
        }
    } else {
        if let Some(reject) = reject_org_oauth_management(state, tenant).await {
            return Err(reject);
        }
        // Org governance decision (2026-08-04), piece 1 — `reject_org_slack_
        // non_admin` used to run here unconditionally, back when Slack was
        // the ONLY provider an org account could reach past the gate above.
        // **Removed (m365 enterprise train PR 3.3):** this arm now also
        // serves Microsoft mints (`reject_org_oauth_management` admits an org
        // caller when the microsoft flag is on), and per-member Outlook
        // connect is deliberately self-service — a Slack-only admin rule
        // applied provider-blind here would wrongly block every non-admin
        // org member's Microsoft connect. Slack's admin-only enforcement is
        // NOT weakened: it is re-checked where the provider IS known —
        // Slack's own connect/disconnect routes call `reject_org_slack_non_
        // admin` themselves, and `callback`'s org Slack arm re-checks the
        // signed state uid's `account_members.role` from the DB (fail-closed;
        // see that check), closing the generic-flow bypass the bare removal
        // would otherwise open. This `else` arm is now ALSO what an
        // admin-flow Slack mint (bot install, no `?surface=`) falls through
        // to — unchanged from before `is_slack_member_request` existed.
    }

    let secret = match oauth_state::state_secret() {
        Some(s) => s,
        None => {
            tracing::error!("OAUTH_STATE_SECRET missing or empty");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "oauth state signing not configured"})),
            )
                .into_response());
        }
    };

    let now = Utc::now().timestamp();
    let nonce = Uuid::new_v4().to_string();
    // CS-454: a Slack member request embeds the RAW `slack_member` literal
    // as the state's `sfc` claim, never routed through `connectors::google::
    // Surface` (which has no such variant and would silently drop it — same
    // reasoning as `is_slack_member_request`'s own comment above).
    let surface_claim: Option<&str> = if let Some(claim) = workspace_claim.as_deref() {
        Some(claim)
    } else if is_slack_member_request {
        Some(SLACK_MEMBER_SURFACE)
    } else if is_slack_install_request {
        Some(SLACK_INSTALL_SURFACE)
    } else {
        surface.map(|s| s.as_str())
    };
    let signed = match return_to {
        Some(ret) => oauth_state::sign_state_for_connect(
            &secret,
            user_ctx.account_id,
            now,
            &nonce,
            Some(user_ctx.user_id),
            surface_claim,
            ret.wire(),
        ),
        None => oauth_state::sign_state(
            &secret,
            user_ctx.account_id,
            now,
            &nonce,
            Some(user_ctx.user_id),
            surface_claim,
        ),
    };
    signed.map_err(|e| {
        tracing::error!(error = %e, "failed to sign oauth state");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response()
    })
}

// ---------------------------------------------------------------------------
// GET /v1/oauth/connections
// ---------------------------------------------------------------------------

/// List the OAuth providers connected to the authenticated account.
///
/// Returns provider metadata only — **no token material**.
pub async fn list_connections(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    // Three independent org gates, any one is enough to proceed (gsuite
    // Train G2 PR 6; Chat arm gsuite Train G4 PR 6, "G4.6"): this route is
    // provider-blind (no `provider` param at all), so an org account whose
    // ONLY enabled connector is Gmail or Chat (`ORG_SLACK_OAUTH_ENABLED`
    // off, `ORG_GOOGLE_GMAIL_ENABLED`/`ORG_GOOGLE_CHAT_ENABLED` on for this
    // org) must still be able to see its own connection here.
    //
    // Resolves ALL THREE flags itself and logs exactly ONE "refused" line
    // for the FINAL decision — CodeRabbit round 1 on #695: this used to call
    // `reject_org_oauth_management` as a probe, which logs its own
    // "refused" line via an internal `tracing::info!` BEFORE this handler
    // ever consulted the Gmail gate, so an org with Slack off but Gmail on
    // logged a spurious refusal on every successful request. Chat resolves
    // its OWN flag directly here too, for the identical reason — a probe
    // call into `reject_org_chat_management` would reproduce the exact bug
    // shape CodeRabbit caught, just for a third flag instead of a second.
    //
    // `slack_visible`/`gmail_visible`/`chat_visible` (CodeRabbit round 2 on
    // #695, Minor — extended to Chat by this PR): kept in scope past this
    // block, not just OR'd into a throwaway `allowed` — the row-level filter
    // below needs each flag's OWN answer. Without it, a per-provider-flag
    // row leak was real: this route's ONE combined gate lets the request
    // through the moment ANY flag is on, but the query below (`WHERE
    // account_id = $1 AND (owner_user_id IS NULL OR owner_user_id = $2)`) is
    // provider-blind — it returns every provider's rows for this account
    // regardless of which flag actually let the caller in. Before gsuite
    // Train G2 PR 6 that was harmless (Slack was the ONLY provider ever able
    // to write an org row, gated on the SAME flag this route's OLD single
    // gate checked), but now that Gmail and Chat can too, an org with
    // `ORG_SLACK_OAUTH_ENABLED` on and `ORG_GOOGLE_GMAIL_ENABLED`/
    // `ORG_GOOGLE_CHAT_ENABLED` off — reachable via Slack alone — would
    // otherwise list a Gmail/Chat row (e.g. left over from before an admin
    // turned that flag back off) as if it were still enabled for this org.
    //
    // Deliberately NOT extended to refuse the whole ROUTE when a row exists
    // for a currently-disabled provider — filtered OUT of the list, not a
    // 403. See [`delete_connection`]'s own comment for why DISCONNECTING an
    // existing row must never depend on any flag: hiding a row from this
    // listing must not also make it unrevokable.
    // Google Calendar org lift (CodeRabbit): an org with ONLY the Calendar
    // flag on now passes the gate below, so a leftover Microsoft member row
    // must not ride the `_ => true` fallback into the listing while the
    // Microsoft flag is off. Personal plane: always visible.
    let microsoft_visible = match tenant.org_context() {
        None => true,
        Some(ctx) => {
            crate::routes::teams_ingest::org_microsoft_teams_enabled_for(&state, &ctx.org_id).await
        }
    };
    let (slack_visible, gmail_visible, chat_visible, meet_visible, github_visible, calendar_visible) = match tenant.org_context() {
        None => (true, true, true, true, true, true),
        Some(ctx) => {
            let slack_ok = org_slack_oauth_enabled_for(&state, &ctx.org_id).await;
            let gmail_ok = org_google_gmail_enabled_for(&state, &ctx.org_id).await;
            let chat_ok = org_google_chat_enabled_for(&state, &ctx.org_id).await;
            // Meet org-plane lift: a fifth independent flag, same "resolve
            // directly here, never probe a `reject_org_*` function" rule.
            let meet_ok = org_google_meet_enabled_for(&state, &ctx.org_id).await;
            // GitHub connector PR A: a fourth independent flag, same
            // "resolve directly here, never probe `reject_org_oauth_
            // management`" discipline as the three above — see this
            // function's own comment for the exact spurious-log bug that
            // discipline avoids.
            let github_ok = crate::connector_flags::github_enabled(&state, &ctx.org_id).await;
            // Google Calendar org lift: a sixth independent flag, same
            // "resolve directly here, never probe a `reject_org_*` function"
            // rule.
            let calendar_ok = org_google_calendar_enabled_for(&state, &ctx.org_id).await;
            if !slack_ok && !gmail_ok && !chat_ok && !meet_ok && !github_ok && !calendar_ok {
                tracing::info!(
                    org_id = %ctx.org_id,
                    "oauth connection route refused under org context (personal-workspace-only in v1)"
                );
                return org_management_refused_response();
            }
            (slack_ok, gmail_ok, chat_ok, meet_ok, github_ok, calendar_ok)
        }
    };
    // `oauth_tokens` is a CONTROL-ONLY table (org-slack step 5, the
    // token-plane decision) — `tenant.pool` is the WRONG handle once an org
    // account can reach this route (`ORG_SLACK_OAUTH_ENABLED`): under
    // `TenancyDecision::Org` it resolves to the org's TENANT pool, and while
    // `oauth_tokens` exists there as a schema artifact of the tenant
    // baseline dump, nothing on this codepath ever writes it on that plane —
    // reading it there would silently return zero rows instead of an org's
    // real (control-plane) connections. `state.control_db()` is the exact
    // pool `callback` below already writes through. Personal / flag-off
    // requests are unaffected: this was always the same physical pool
    // `tenant.pool` resolved to for them.
    let pool = match state.control_db() {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };

    // RLS scoping (org-tenancy conversion): `oauth_tokens` reads run inside
    // one `begin_account_scoped` transaction per request (inert pre-OPS-4).
    let mut tx = match crate::tenancy::scope::begin_request_account_read(&pool, user_ctx.account_id).await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user_ctx.account_id, "failed to open scoped tx for oauth connections list");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    // Foundation PR 1.1 originally scoped this page to the caller's own
    // ACCOUNT-WIDE connections only (`owner_user_id IS NULL` —
    // `TokenOwner::AccountWide`), with a comment noting a per-member
    // connection "gets its own management surface when that lands" (Train 3).
    // gsuite Train G2 PR 1, item 5: that day is here for Gmail — this now
    // ALSO returns the caller's own `Member` row(s) (`owner_user_id =
    // user_ctx.user_id`).
    //
    // This is NOT the "IS NULL OR owner_user_id = $n" fallback `TokenOwner`'s
    // own doc comment forbids — that rule protects a CREDENTIAL READ (e.g.
    // `load_creds`) from ever handing a member the wrong owner's token
    // instead of their own. This is a metadata LISTING whose whole purpose is
    // to show the caller BOTH categories they are entitled to see: every
    // shared account-wide connection (visible to anyone in the account, by
    // definition of `AccountWide`) plus their OWN personal connection(s) —
    // never another member's. `$2` is always the AUTHENTICATED caller's own
    // id, so this can never surface a different member's row.
    //
    // Order within a provider = the order a sync LOADS a token in, so the
    // first row per provider (what the connectors page reads for its status
    // and reconnect notice) is the credential actually in use: the caller's
    // OWN `Member` row before an account-wide one (`routes::connectors::
    // load_creds_owner_aware` tries `Member` first for Microsoft; a Google
    // org member's Calendar token is their `Member` row), then newest
    // `updated_at` first (`load_creds`'s own `ORDER BY updated_at DESC
    // LIMIT 1`). Oldest-first let a legacy account-wide row beside the
    // member's own, or a revoked row left behind by a reconnect under a
    // different provider account, decide the notice — so it either never
    // showed or never cleared.
    let rows = sqlx::query(
        r#"
        SELECT provider, provider_workspace_id, scope, expires_at, created_at,
               credential_invalid_at, credential_invalid_reason,
               COALESCE(owner_user_id = $2, false) AS mine
        FROM oauth_tokens
        WHERE account_id = $1 AND (owner_user_id IS NULL OR owner_user_id = $2)
        ORDER BY provider, (owner_user_id IS NULL), updated_at DESC
        "#,
    )
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .fetch_all(&mut *tx)
    .await;
    tx.rollback().await.ok();

    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user_ctx.account_id, "failed to list oauth connections");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    // Row-level visibility filter (CodeRabbit round 2 on #695, Minor —
    // extended to `google_chat` by gsuite Train G4 PR 6; see this fn's own
    // `slack_visible`/`gmail_visible`/`chat_visible` comment above for the
    // exact leak this closes). `slack_visible`/`gmail_visible`/`chat_visible`
    // are `true` unconditionally on the Personal plane, so this `filter` is a
    // no-op there — byte-identical to before either fix. Any provider OTHER
    // than `"slack"`/`"slack_user"`/`"google_gmail"`/`"google_chat"` is left
    // unfiltered: Calendar (`provider == "google"`) is the only other
    // org-reachable-in-principle literal today and stays grandfathered/
    // `AccountWide`-only per `connectors::google::Surface::is_grandfathered`'s
    // own doc, with no connector-flag dimension to filter on in the first
    // place.
    //
    // `"slack_user"` (CS-454, item 46a): gated on the SAME `slack_visible`
    // flag as the bot row — an org with Slack disabled should not surface a
    // member's own leftover connection any more than it surfaces the bot's.
    let connections: Vec<OAuthConnection> = rows
        .iter()
        .filter(|row| {
            let provider: &str = row.get("provider");
            match provider {
                "slack" | "slack_user" => slack_visible,
                "google_gmail" => gmail_visible,
                "google_chat" => chat_visible,
                "google_meet" => meet_visible,
                "microsoft" => microsoft_visible,
                "github" => github_visible,
                // Calendar's grandfathered literal (Google Calendar org
                // lift): `calendar_visible` is `true` unconditionally on the
                // personal plane, and under an org follows the org's
                // `google_calendar` flag — a leftover member row from before
                // the flag was turned off is hidden, never unrevokable
                // (`delete_connection` has no flag gate).
                "google" => calendar_visible,
                _ => true,
            }
        })
        .map(|row| {
            let provider: String = row.get("provider");
            let scope: String = row.get("scope");
            let (missing_scopes, scopes_unknown) = scope_reconsent_status(&provider, &scope);
            OAuthConnection {
                provider_workspace_id: row.get("provider_workspace_id"),
                expires_at: row.get("expires_at"),
                connected_at: row.get("created_at"),
                missing_scopes,
                scopes_unknown,
                credential_invalid_at: row.get("credential_invalid_at"),
                credential_invalid_reason: row.get("credential_invalid_reason"),
                mine: row.get("mine"),
                provider,
                scope,
            }
        })
        .collect();

    (StatusCode::OK, Json(connections)).into_response()
}

// ---------------------------------------------------------------------------
// DELETE /v1/oauth/connections/:provider
// ---------------------------------------------------------------------------

/// Revoke an OAuth connection.
///
/// Deletes the token row(s) from `oauth_tokens` for every workspace this
/// account connected under `provider`. For `provider = 'slack'` it ALSO
/// deletes the legacy `slack_integrations` row(s) for those workspaces, then
/// removes the matching `connector_installs` routing row(s) (migration
/// `0121_connector_installs`) — the surviving `slack_integrations` guard is
/// now satisfied by construction for slack, and still protects any other
/// provider. Provider-side revocation is attempted on a best-effort basis
/// (failure does not affect the response).
///
/// # Why this deletes `slack_integrations` too
///
/// It used to leave that row alone, on the reasoning that the legacy
/// `routes/slack.rs::oauth_callback` owns it and `routes/slack.rs::
/// delete_integration` is the route that tears it down. Structurally tidy,
/// and wrong in practice: **this is the only disconnect the UI exposes**
/// (`web/lib/api.ts`'s `oauth.disconnect`), and `delete_integration` has no
/// caller in the app at all. So "Disconnect" deleted the token row nothing
/// on the ingest path reads — `ingest_slack_message` takes its bot token
/// from `slack_integrations.bot_token` — while this function's own
/// `connector_installs` guard kept the routing row alive precisely BECAUSE
/// `slack_integrations` survived. Messages kept flowing into the user's
/// ocean after they disconnected. Huddle capture did stop (it reads
/// `oauth_tokens`), which made the disconnect look partly effective and hid
/// the half still running.
///
/// A user withdrawing consent must actually stop the data flow, so the row
/// that powers ingest is torn down here rather than in a route nothing calls.
/// `delete_integration` keeps its own mirror-image guard and still works for
/// any direct API caller.
///
/// `provider = "slack_user"` (CS-454, item 46a) is handled by an early,
/// SEPARATE branch — [`delete_slack_member_connection`] — before any of the
/// shared logic below runs. Deliberately narrow rather than folded into the
/// generic multi-row DELETE this function otherwise uses: that DELETE's own
/// `(owner_user_id IS NULL OR owner_user_id = $3)` shape exists for
/// providers with a genuine `AccountWide` row to also match (Calendar,
/// Slack's own bot row); a `slack_user` row never has one, so the correct
/// shape is a plain owner-EXACT match with no `OR` at all. Keeping it as an
/// early return, rather than adding a third arm to the shared query,
/// leaves the shared multi-provider DELETE (and any concurrent work
/// touching it) untouched.
pub async fn delete_connection(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(provider): Path<String>,
) -> Response {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    // CS-454, item 46a — see this function's own doc comment above.
    if provider == "slack_user" {
        return delete_slack_member_connection(&state, &tenant, &user_ctx).await;
    }
    // **No connector-flag gate here at all (CodeRabbit round 2 on #695,
    // Minor — deliberate design decision, not an oversight).** Earlier
    // revisions of this PR gated disconnect the same way `list_connections`
    // gates its LISTING (Slack flag OR Gmail flag, `provider ==
    // "google_gmail"`-restricted). Precedent check: before gsuite Train G2
    // PR 6, Slack was the ONLY provider ever reachable here at all, and it
    // WAS flag-gated — so if an admin turned `ORG_SLACK_OAUTH_ENABLED` off
    // for an org with an existing live Slack connection, no member could
    // revoke it through the product (`routes::slack::delete_integration`,
    // the only other Slack-disconnect route, has no caller in the app — see
    // this function's own module-level doc). That is an orphaned,
    // unrevokable-in-product token, not a deliberate safety property: a
    // connector flag governs whether an org may CREATE a new connection
    // (`callback`'s own org/provider gate, `mint_state`/`authorize`'s
    // gates) — it was never meant to also strip a member's ability to undo
    // one that already exists. Gating disconnect on it was carrying that
    // implication by accident, not by design; gsuite Train G2 PR 6 is the
    // first time two DIFFERENT flags share this one route, which is what
    // surfaced the question. So: this route now disconnects whatever
    // `oauth_tokens` row the caller is entitled to remove — the `DELETE ...
    // WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NOT
    // DISTINCT FROM $3` below (`$3` resolved by
    // [`resolve_delete_token_owner`], CS-452) is the real authorization
    // boundary (self-scoped to this account, and to EXACTLY the `TokenOwner`
    // shape this provider/plane resolves to — never both an `AccountWide`
    // row and the caller's own `Member` row in the same statement, which is
    // what the pre-CS-452 `owner_user_id IS NULL OR owner_user_id = $3`
    // fallback allowed; see that function's own doc for the incident shape
    // this closes) — REGARDLESS of whether `list_connections`
    // currently shows that row (see that function's own `slack_visible`/
    // `gmail_visible`/`chat_visible` comment: a row can be legitimately
    // hidden from the list while its flag is off, and must still be
    // revocable here). Gmail (gsuite Train G2 PR 6) and Chat (gsuite Train
    // G4 PR 6, "G4.6") CAN now have an org row, same as Slack — this
    // function's own gate stays unconditional regardless, unaffected by
    // either lift, and Meet (Meet org-plane lift) can now have one too.
    // Only Calendar still can never have one (grandfathered `AccountWide`-only,
    // contract §1.8) and simply finds zero rows, 404ing below, same as
    // always.
    // Org governance decision (2026-08-04), piece 1: Slack is the only
    // provider whose disconnect is admin-only under an org (org governance
    // decision 2026-08-04, piece 1 — a DIFFERENT axis entirely from the
    // connector-flag question above: Slack's shared workspace bot token is
    // an org-wide asset, so tearing it down is restricted to admins/owners,
    // regardless of which flags are on). Gmail (gsuite Train G2 PR 6) is
    // per-member consent, not an org-wide admin action, so it does NOT get
    // this admin-only check (same reasoning as `mint_state`'s own Gmail
    // branch skipping `reject_org_slack_non_admin`). Explicit `provider ==
    // "slack"` check, not unconditional, so a future non-Slack org provider
    // does not silently inherit an admin-only rule that was never written
    // for it.
    if provider == "slack" {
        if let Some(reject) = reject_org_slack_non_admin(&tenant, &user_ctx.role) {
            return reject;
        }
    }
    // Same post-gate pool resolution as `list_connections` — CONTROL, not
    // `tenant.pool`. See that function's doc comment.
    let pool = match state.control_db() {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };

    // Teams doorbell (S5b): if this member owns a shared channel subscription
    // and nobody else can take it, delete that Graph subscription with their
    // token BEFORE the `oauth_tokens` row below is deleted. A successor
    // PATCHes the existing id instead. Other members' watches are not touched.
    if provider == "microsoft" {
        if let Ok(_plane) =
            crate::crypto::DekPlane::resolve_for_account(&state, &pool, user_ctx.account_id).await
        {
            let graph = crate::connectors::teams_doorbell::HttpDoorbell {
                http: &state.http_client,
            };
            let tokens = crate::connectors::teams_doorbell::ResolvedWatcherTokens {
                state: &state,
                control: &pool,
                http: &state.http_client,
            };
            let org = tenant.org_context().is_some();
            use crate::connectors::teams_doorbell::WatcherTokens as _;
            let departing = tokens
                .access_token(user_ctx.account_id, user_ctx.user_id, org)
                .await;
            if let Err(e) = crate::connectors::teams_doorbell::prepare_owner_departure(
                &pool,
                &graph,
                &tokens,
                user_ctx.account_id,
                user_ctx.user_id,
                departing.as_deref(),
            )
            .await
            {
                tracing::warn!(
                    error = %e,
                    account_id = %user_ctx.account_id,
                    "microsoft disconnect: teams doorbell handoff failed — the token delete still proceeds"
                );
            }
        }
    }

    // Fence Google cleanup BEFORE token removal/provider latency. A
    // reconnect's new watch or explicit approval must survive this request.
    let google_purge_clocks = if matches!(provider.as_str(), "google" | "google_gmail" | "google_chat" | "google_meet") {
        let control = sqlx::query_scalar::<_,DateTime<Utc>>("SELECT clock_timestamp()").fetch_one(&pool).await;
        let content = match tenant.pool.as_ref() {
            Some(content) => sqlx::query_scalar::<_,DateTime<Utc>>("SELECT clock_timestamp()").fetch_one(content).await,
            None => Err(sqlx::Error::Protocol("missing content pool".into())),
        };
        Some((control,content))
    } else { None };
    // Same one-scoped-tx-per-request pattern as `list_connections`.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(&pool, user_ctx.account_id).await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %user_ctx.account_id, "failed to open scoped tx for oauth connection delete");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    // RETURNING the removed workspace ids (AND, Foundation PR 1.5, the
    // encrypted `access_token` each row carried): `connector_installs`
    // maintenance below needs to know exactly which workspaces just lost
    // their `oauth_tokens` row, and the "not found" check below needs the
    // same count `rows_affected()` used to give — a plain DELETE loses both.
    // Foundation PR 1.1 originally scoped this to account-wide-only
    // (`TokenOwner::AccountWide` — `owner_user_id IS NULL`). gsuite Train G2
    // PR 1, item 5 widened it to ALSO delete the caller's OWN `Member`
    // row(s) — but via the `owner_user_id IS NULL OR owner_user_id = $3`
    // fallback shape `TokenOwner`'s own doc comment says must never exist:
    // in one statement that matched BOTH the shared `AccountWide` row (if
    // one existed for this provider) AND the caller's own `Member` row,
    // so a member disconnecting a per-member provider (Gmail, Chat,
    // org-plane Microsoft) could delete an `AccountWide` connection every
    // OTHER member still depends on. CS-452 fixes this: `$3` is now
    // [`resolve_delete_token_owner`]'s single, exact answer for
    // `(provider, tenant.org_context().is_some())` — `NULL` for a provider
    // resolved `AccountWide` (Calendar, Slack, personal-plane Microsoft),
    // the AUTHENTICATED caller's own id for one resolved `Member` (Gmail,
    // Chat, Meet, GitHub, org-plane Microsoft) — never both, never another
    // member's id (never a client parameter either way). Calendar
    // (`owner_user_id IS NULL`, account-wide) stays disconnectable by any
    // account member, byte-identical to before.
    //
    // Foundation PR 1.5: this used to be a bare `RETURNING provider_workspace_id`,
    // with a SEPARATE `SELECT bot_token FROM slack_integrations` below to
    // capture the plaintext token for the purge's `conversations.leave` step
    // before that row was deleted. That second read is gone: this ONE
    // `RETURNING` now also captures the encrypted `access_token` each removed
    // row carried, decrypted AFTER commit (see below) — the same "decrypt
    // happens after the transaction closes" discipline
    // `workers::huddle::get_slack_bot_token` documents for itself. This also
    // means a decrypt failure can no longer poison an in-flight transaction:
    // by the time it would happen the delete is already committed, so it
    // degrades to "skip conversations.leave for this workspace" instead of
    // aborting the whole disconnect. CS-452 reuses this SAME capture for
    // Microsoft's own post-commit decrypt (see the Microsoft purge call
    // site below) — the encrypted `access_token` a removed Microsoft row
    // carried is the disconnecting owner's own delegated Graph token, needed
    // to delete a delegated subscription upstream before it is orphaned.
    let delete_owner =
        resolve_delete_token_owner(&provider, tenant.org_context().is_some(), user_ctx.user_id);
    let removed_rows: Result<Vec<(Option<String>, Vec<u8>)>, sqlx::Error> = sqlx::query_as(
        "DELETE FROM oauth_tokens WHERE account_id = $1 AND provider = $2 \
         AND owner_user_id IS NOT DISTINCT FROM $3 \
         RETURNING provider_workspace_id, access_token",
    )
    .bind(user_ctx.account_id)
    .bind(&provider)
    .bind(delete_owner.owner_user_id())
    .fetch_all(&mut *tx)
    .await;

    let removed_rows = match removed_rows {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %user_ctx.account_id,
                provider = %provider,
                "failed to delete oauth connection"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    if removed_rows.is_empty() {
        tx.rollback().await.ok();
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "connection not found"})),
        )
            .into_response();
    }

    // Encrypted `access_token`s, keyed by workspace — decrypted after commit,
    // only for `provider == "slack"` (see the purge step below). Kept
    // alongside `workspace_ids` (not folded into it) so the common,
    // non-Slack path pays nothing extra.
    // gsuite Train G4 PR 4: Chat's best-effort revoke must NOT be read out of
    // the workspace-keyed map below, because Chat's revoke is keyed on
    // `(account_id, owner_user_id)` and never on a workspace. Two properties
    // of that map break it: `provider_workspace_id` is nullable
    // (`Option<String>` above), so a row carrying NULL is dropped from the
    // map entirely and the revoke is then skipped SILENTLY; and the map is
    // keyed by workspace id, so two removed rows sharing one collide and a
    // token is discarded. Capture the encrypted tokens directly instead —
    // the common, non-Chat path pays one branch and allocates nothing.
    let removed_encrypted_chat_tokens: Vec<Vec<u8>> = if provider == "google_chat" || provider == "google_meet" {
        removed_rows
            .iter()
            .map(|(_, token)| token.clone())
            .collect()
    } else {
        Vec::new()
    };

    let mut removed_encrypted_tokens: std::collections::HashMap<String, Vec<u8>> =
        std::collections::HashMap::new();
    let mut workspace_ids: Vec<String> = Vec::with_capacity(removed_rows.len());
    for (ws, token) in removed_rows {
        if let Some(ws) = ws {
            removed_encrypted_tokens.insert(ws.clone(), token);
            workspace_ids.push(ws);
        }
    }
    // Encrypted `slack_user` tokens torn down with the bot disconnect
    // (CS-454 CodeRabbit). Decrypted after commit for best-effort
    // `auth.revoke`, same post-commit discipline as `removed_encrypted_tokens`.
    let mut removed_encrypted_member_tokens: Vec<Vec<u8>> = Vec::new();

    // ── Slack: also drop the legacy `slack_integrations` row ────────────────
    //
    // MUST run before the `connector_installs` delete below, whose guard is
    // "keep the install row if `slack_integrations` still has one".
    //
    // This route is the ONLY disconnect the UI exposes (`web/lib/api.ts`'s
    // `oauth.disconnect` → `DELETE /v1/oauth/connections/:provider`;
    // `routes::slack::delete_integration` has no caller in the app). Deleting
    // only `oauth_tokens` therefore did NOT disconnect Slack:
    //
    //   * `ingest_slack_message` reads its bot token from
    //     `slack_integrations.bot_token`, NOT from `oauth_tokens` — a
    //     different source from `workers::huddle::get_slack_bot_token`. That
    //     row survived, so the token stayed valid.
    //   * `connector_installs` survived too — by this function's own guard,
    //     precisely BECAUSE `slack_integrations` still existed — so
    //     `resolve_slack_account_ids` kept matching the workspace to the
    //     account.
    //
    // Net effect: a user clicked "Disconnect", saw the connector disappear
    // from the page, and their Slack messages kept being ingested into their
    // ocean. Huddle capture stopped (it reads `oauth_tokens`), which made the
    // disconnect look partly effective and hid the half that kept running.
    // Consent withdrawn in the UI must stop the data flow, so the row that
    // actually powers ingest goes with it.
    //
    if provider == "slack" && !workspace_ids.is_empty() {
        let legacy_delete = sqlx::query(
            "DELETE FROM slack_integrations \
             WHERE account_id = $1 AND workspace_id = ANY($2)",
        )
        .bind(user_ctx.account_id)
        .bind(&workspace_ids)
        .execute(&mut *tx)
        .await;
        match legacy_delete {
            Ok(r) => {
                if r.rows_affected() > 0 {
                    tracing::info!(
                        account_id = %user_ctx.account_id,
                        rows = r.rows_affected(),
                        "slack disconnect: removed legacy slack_integrations row(s) — the bot \
                         token ingest actually reads"
                    );
                }
            }
            // Fail the whole disconnect rather than commit a half-disconnect.
            // Unlike the `connector_installs` cleanup below (whose worst case
            // is a stale routing row), leaving this one behind means we keep
            // ingesting for a user who asked us to stop.
            Err(e) => {
                tracing::error!(
                    error = %e,
                    account_id = %user_ctx.account_id,
                    "slack disconnect: failed to delete slack_integrations — aborting so the \
                     caller retries rather than leaving ingest live after a disconnect"
                );
                tx.rollback().await.ok();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        }
    }

    if !workspace_ids.is_empty() {
        // Guard: for `provider = 'slack'` specifically, keep the
        // `connector_installs` row alive if `slack_integrations` still has a
        // live row for the same (account_id, workspace) — see this
        // function's own doc comment.
        //
        // `ci.owner_user_id IS NOT DISTINCT FROM $4` (CodeRabbit round-2 fix
        // on this PR; tightened further on CodeRabbit review of #1052): the
        // `oauth_tokens` DELETE above is owner-EXACT (`resolve_delete_token_
        // owner`), but this `connector_installs` DELETE originally used the
        // `IS NULL OR = $4` fallback shape — it matched on `(account_id,
        // provider, external_workspace_id)` alone, then OR'd in both the
        // account-wide and the caller's-own-id cases in one statement.
        // `connector_installs`' own per-owner partial unique index
        // (`uq_connector_installs_per_owner`, migration 0121) explicitly
        // allows TWO DIFFERENT owners to hold a row for the SAME
        // `(provider, external_workspace_id, account_id)` triple — so the
        // old fallback could remove a DIFFERENT owner's install row sharing
        // that workspace id (e.g. a legacy AccountWide Microsoft install row
        // alongside per-member org-plane rows for the SAME AAD tenant,
        // mirroring the exact `oauth_tokens` coexistence
        // `resolve_delete_token_owner`'s own doc comment names) — Microsoft
        // routing self-heals through the surviving `oauth_tokens` row either
        // way (`resolve_connector_install_account_ids`'s own fallback), so
        // this was a routing-index miss, not an orphaned connection, but the
        // fix is the identical one-line tightening the `oauth_tokens` DELETE
        // above already got, so there is no reason to leave it looser here.
        // `$4` is `delete_owner.owner_user_id()` — the SAME exact value the
        // `oauth_tokens` DELETE above just used — never a client parameter.
        let install_delete = sqlx::query(
            "DELETE FROM connector_installs ci \
             WHERE ci.account_id = $1 AND ci.provider = $2 \
               AND ci.external_workspace_id = ANY($3) \
               AND ci.owner_user_id IS NOT DISTINCT FROM $4 \
               AND ($2 <> 'slack' OR NOT EXISTS ( \
                     SELECT 1 FROM slack_integrations si \
                     WHERE si.account_id = $1 AND si.workspace_id = ci.external_workspace_id \
                   ))",
        )
        .bind(user_ctx.account_id)
        .bind(&provider)
        .bind(&workspace_ids)
        .bind(delete_owner.owner_user_id())
        .execute(&mut *tx)
        .await;
        // NOT "non-fatal", which is what this used to log. A failed statement
        // ABORTS the enclosing Postgres transaction: every later command in it
        // errors, and `COMMIT` on an aborted transaction is executed as
        // ROLLBACK and returns SUCCESS. So swallowing this into a `warn!` and
        // falling through to `tx.commit()` silently discarded the
        // `oauth_tokens` and `slack_integrations` deletes as well, then
        // answered `200 OK, disconnected` — the user is told they are
        // disconnected and NOTHING was removed, which is a worse version of
        // the very bug this PR exists to fix.
        //
        // Verified rather than assumed: BEGIN; DELETE (ok); DELETE (error);
        // COMMIT; leaves the first DELETE's rows in place, with no error
        // raised by the COMMIT itself.
        if let Err(e) = install_delete {
            tracing::error!(
                error = %e,
                account_id = %user_ctx.account_id,
                provider = %provider,
                "failed to delete connector_installs row(s) on oauth disconnect — aborting the \
                 whole disconnect (the transaction is already poisoned, so committing here would \
                 silently revert the token deletes and still report success)"
            );
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // CS-454 CodeRabbit: an admin bot disconnect must also tear down
    // per-owner `slack` connector_installs and `slack_user` oauth_tokens for
    // the workspaces just removed. Member `connector_installs` are written
    // with `provider = 'slack'` and `owner_user_id = member`; the owner-scoped
    // DELETE above (`owner_user_id IS NULL OR = admin`) leaves those rows,
    // which keep `find_other_org_slack_workspace` matching the old workspace
    // (blocking reconnect) and keep inbound events routable after consent
    // was withdrawn. Scoped to these workspaces only — unrelated providers
    // and workspaces are not touched. Revoke is best-effort after commit.
    if provider == "slack" && !workspace_ids.is_empty() {
        let member_tokens = sqlx::query_scalar::<_, Vec<u8>>(
            "DELETE FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack_user' \
               AND provider_workspace_id = ANY($2) \
             RETURNING access_token",
        )
        .bind(user_ctx.account_id)
        .bind(&workspace_ids)
        .fetch_all(&mut *tx)
        .await;
        match member_tokens {
            Ok(tokens) => removed_encrypted_member_tokens = tokens,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    account_id = %user_ctx.account_id,
                    "slack disconnect: failed to delete slack_user oauth_tokens — aborting so \
                     the caller retries rather than leaving member rows that still route the \
                     old workspace"
                );
                tx.rollback().await.ok();
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response();
            }
        }

        if let Err(e) = sqlx::query(
            "DELETE FROM connector_installs \
             WHERE account_id = $1 AND provider = 'slack' \
               AND external_workspace_id = ANY($2) \
               AND owner_user_id IS NOT NULL",
        )
        .bind(user_ctx.account_id)
        .bind(&workspace_ids)
        .execute(&mut *tx)
        .await
        {
            tracing::error!(
                error = %e,
                account_id = %user_ctx.account_id,
                "slack disconnect: failed to delete per-owner slack connector_installs — \
                 aborting so the caller retries rather than leaving member routing rows"
            );
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    // Connector compliance audit (m365 enterprise train PR 4.2): the
    // `disconnect` row lands in the SAME transaction as the `oauth_tokens`/
    // `slack_integrations`/`connector_installs` deletes above — durability
    // tier 1, the hard bar. The best-effort tenant-plane PURGE step below
    // (watched-channel/subscription cleanup) is audited separately, after
    // commit, with the weaker `record_or_queue` posture that step's own
    // existing best-effort convention already accepts.
    if let Err(e) = crate::tenancy::scope::set_tenant_user(&mut tx, user_ctx.user_id).await {
        tracing::error!(error = %e, account_id = %user_ctx.account_id, "failed to set app.user_id for connector_audit_log write");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response();
    }
    if let Err(e) = crate::services::connector_audit::record_in_tx(
        &mut tx,
        crate::services::connector_audit::AuditEntry {
            account_id: user_ctx.account_id,
            actor: crate::services::connector_audit::Actor::User(user_ctx.user_id),
            action: "disconnect",
            provider: Some(provider.clone()),
            subject: workspace_ids.first().cloned(),
            detail: Some(json!({"workspace_count": workspace_ids.len()})),
        },
    )
    .await
    {
        tracing::error!(
            error = %e,
            account_id = %user_ctx.account_id,
            provider = %provider,
            "failed to write connector_audit_log 'disconnect' row — failing the disconnect \
             rather than committing an unaudited one"
        );
        tx.rollback().await.ok();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to persist audit record"})),
        )
            .into_response();
    }

    if let Err(e) = tx.commit().await {
        tracing::warn!(
            error = %e,
            account_id = %user_ctx.account_id,
            provider = %provider,
            "failed to commit oauth connection delete"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response();
    }

    if let Err(e)=crate::read_models::dirty::connector_changed(&state,&tenant,user_ctx.account_id,user_ctx.user_id).await {
        tracing::error!(error=%e,"OAuth revoke read-model invalidation failed");
        return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"read model invalidation failed"}))).into_response();
    }
    tracing::info!(
        account_id = %user_ctx.account_id,
        provider = %provider,
        "OAuth connection revoked"
    );

    // Foundation PR 1.5: decrypt the `access_token`s captured by the
    // `RETURNING` clause above, now that the delete they came from has
    // committed. Only for `provider == "slack"` — the purge step just below
    // is the sole consumer. A DEK/decrypt failure is soft here (unlike the
    // pre-PR-1.5 pre-commit read this replaces): the delete already
    // committed, so the disconnect itself is unaffected — only the
    // best-effort `conversations.leave` call is skipped for the affected
    // workspace(s).
    let mut slack_bot_tokens: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // The DEK these rows were ENCRYPTED under, which for an org account is
    // its tenant DB's `account_keys` — not `accounts.dek_wrapped` (org-plane
    // DEK fix; this route is genuinely reachable under an org
    // `TenancyDecision`, which is why the gate above deliberately does not
    // refuse one). Unlike the callback's own resolution this needs no routing
    // lookup: `delete_connection` runs behind the binding gate, so the
    // request's own decision already carries the fingerprint-verified tenant
    // pool. Both uses below stay best-effort — a wrong/missing key only skips
    // the upstream courtesy call, it never fails the disconnect.
    let dek_plane = DekPlane::from_decision(tenant.decision.as_ref(), &pool);
    if provider == "slack" && !removed_encrypted_tokens.is_empty() {
        match dek_plane.resolve(user_ctx.account_id).await {
            Ok(Some(dek)) => {
                for (workspace_id, encrypted) in &removed_encrypted_tokens {
                    match decrypt_token(&dek, encrypted) {
                        Ok(token) => {
                            slack_bot_tokens.insert(workspace_id.clone(), token);
                        }
                        Err(e) => tracing::warn!(
                            error = %e,
                            account_id = %user_ctx.account_id,
                            workspace_id = %workspace_id,
                            "slack disconnect: failed to decrypt bot token post-commit — \
                             conversations.leave will be skipped for this workspace"
                        ),
                    }
                }
            }
            Ok(None) => tracing::warn!(
                account_id = %user_ctx.account_id,
                "slack disconnect: account has no dek_wrapped provisioned — conversations.leave \
                 will be skipped for every removed workspace"
            ),
            Err(e) => tracing::warn!(
                error = %e,
                account_id = %user_ctx.account_id,
                "slack disconnect: failed to resolve account DEK post-commit — \
                 conversations.leave will be skipped for every removed workspace"
            ),
        }
    }

    // CS-454 CodeRabbit: best-effort `auth.revoke` for member tokens the
    // cascade above just removed. Same post-commit decrypt discipline as
    // the bot-token block — a DEK/decrypt/network failure never undoes the
    // local delete.
    if provider == "slack" && !removed_encrypted_member_tokens.is_empty() {
        match dek_plane.resolve(user_ctx.account_id).await {
            Ok(Some(dek)) => {
                for encrypted in &removed_encrypted_member_tokens {
                    match decrypt_token(&dek, encrypted) {
                        Ok(plaintext) => {
                            if let Err(e) = crate::connectors::slack::revoke_member_token(
                                &state.http_client,
                                &plaintext,
                            )
                            .await
                            {
                                tracing::info!(
                                    error = %e,
                                    account_id = %user_ctx.account_id,
                                    "slack disconnect: member auth.revoke failed (non-fatal)"
                                );
                            }
                        }
                        Err(e) => tracing::warn!(
                            error = %e,
                            account_id = %user_ctx.account_id,
                            "slack disconnect: failed to decrypt a member token post-commit — \
                             auth.revoke skipped for that row (non-fatal)"
                        ),
                    }
                }
            }
            Ok(None) => tracing::warn!(
                account_id = %user_ctx.account_id,
                "slack disconnect: account has no dek_wrapped provisioned — member \
                 auth.revoke skipped (non-fatal)"
            ),
            Err(e) => tracing::warn!(
                error = %e,
                account_id = %user_ctx.account_id,
                "slack disconnect: failed to resolve account DEK post-commit — member \
                 auth.revoke skipped (non-fatal)"
            ),
        }
    }

    // gsuite Train G4 PR 4: the SAME post-commit, decrypt-after-the-
    // transaction-closes shape the Slack block just above uses, applied to
    // Chat's own best-effort upstream revoke. It must run BEFORE the purge
    // below, because the purge deletes the `google_watch_channels` row that
    // carries the subscription name we are about to revoke. Entirely
    // non-fatal — see `google_disconnect::revoke_chat_subscription_best_effort`'s
    // own doc for why Chat is the deliberate exception to that module's
    // "no upstream calls at all" posture.
    if (provider == "google_chat" || provider == "google_meet") && !removed_encrypted_chat_tokens.is_empty() {
        match dek_plane.resolve(user_ctx.account_id).await {
            Ok(Some(dek)) => {
                // EXACTLY ONE upstream call, not one per removed row. The
                // DELETE above matches `owner_user_id IS NULL OR owner_user_id
                // = $3` and so can remove more than one row, but every one of
                // them belongs to this member, and
                // `revoke_chat_subscription_best_effort` resolves the
                // subscription from `(account_id, owner_user_id)` — a second
                // iteration would repeat the identical `google_watch_channels`
                // lookup and the identical `subscriptions.delete`, whose only
                // possible outcome is a 404 logged as a second "revoked" line.
                // A decrypt failure DOES fall through to the next row, because
                // a different row may still carry a usable token.
                for encrypted in &removed_encrypted_chat_tokens {
                    match decrypt_token(&dek, encrypted) {
                        Ok(token) => {
                            let revoked = crate::connectors::google_disconnect::revoke_chat_subscription_best_effort(
                                &state.http_client,
                                &pool,
                                user_ctx.account_id,
                                user_ctx.user_id,
                                &token,
                                if provider == "google_meet" { "meet" } else { "chat" },
                                google_purge_clocks.as_ref().and_then(|(clock,_)| clock.as_ref().ok()).copied(),
                            )
                            .await;
                            tracing::info!(
                                account_id = %user_ctx.account_id, revoked,
                                "google workspace disconnect: best-effort upstream subscriptions.delete"
                            );
                            break;
                        }
                        Err(e) => tracing::info!(
                            error = %e, account_id = %user_ctx.account_id,
                            "google chat disconnect: failed to decrypt the removed access token \
                             post-commit — trying any other removed row, and if none decrypts the \
                             upstream subscriptions.delete is skipped and the subscription \
                             expires on its own within the 7-day TTL"
                        ),
                    }
                }
            }
            Ok(None) | Err(_) => tracing::info!(
                account_id = %user_ctx.account_id,
                "google chat disconnect: no usable DEK post-commit — the upstream \
                 subscriptions.delete is skipped (non-fatal)"
            ),
        }
    }

    // CS-452: the SAME post-commit, decrypt-after-the-transaction-closes
    // shape the Slack/Chat blocks above use, applied to Microsoft's own
    // delegated-subscription upstream delete. Must run BEFORE the Microsoft
    // purge below, which is the only consumer. At most one row is ever
    // removed for `provider == "microsoft"` (the `oauth_tokens` DELETE above
    // is owner-exact — `resolve_delete_token_owner` — so it can never match
    // both an `AccountWide` row and the caller's own `Member` row in one
    // statement), so `removed_encrypted_tokens` holds at most one entry here;
    // which workspace key it lives under does not matter, only that it
    // decrypts. A decrypt failure (or no DEK) degrades to `None`, which
    // `purge_microsoft_connection`'s own `delegated_access_token` parameter
    // treats as "skip the delegated upstream delete" — never fatal to the
    // disconnect, which already committed above.
    let mut microsoft_access_token: Option<String> = None;
    if provider == "microsoft" && !removed_encrypted_tokens.is_empty() {
        match dek_plane.resolve(user_ctx.account_id).await {
            Ok(Some(dek)) => {
                for encrypted in removed_encrypted_tokens.values() {
                    match decrypt_token(&dek, encrypted) {
                        Ok(token) => {
                            microsoft_access_token = Some(token);
                            break;
                        }
                        Err(e) => tracing::info!(
                            error = %e,
                            account_id = %user_ctx.account_id,
                            "microsoft disconnect: failed to decrypt the removed access token \
                             post-commit — the delegated subscription upstream delete will be \
                             skipped (non-fatal; Graph expires the orphan on its own short TTL)"
                        ),
                    }
                }
            }
            Ok(None) => tracing::info!(
                account_id = %user_ctx.account_id,
                "microsoft disconnect: account has no dek_wrapped provisioned post-commit — the \
                 delegated subscription upstream delete will be skipped (non-fatal)"
            ),
            Err(e) => tracing::info!(
                error = %e,
                account_id = %user_ctx.account_id,
                "microsoft disconnect: failed to resolve account DEK post-commit — the delegated \
                 subscription upstream delete will be skipped (non-fatal)"
            ),
        }
    }

    // ── Shared disconnect purge (org governance decision 2026-08-04, piece 3) ──
    // Runs AFTER commit, not inside the transaction above: it writes to
    // `tenant_pool` (a physically different database for an org account),
    // which cannot participate in the control-plane transaction just
    // committed anyway. See `connectors::slack_disconnect`'s own header for
    // the full contract (what it purges, what it deliberately does not).
    if provider == "slack" && !workspace_ids.is_empty() {
        match &tenant.pool {
            Some(tenant_pool) => {
                for workspace_id in &workspace_ids {
                    let outcome = crate::connectors::slack_disconnect::purge_slack_workspace(
                        &state.http_client,
                        &pool,
                        tenant_pool,
                        user_ctx.account_id,
                        workspace_id,
                        slack_bot_tokens.get(workspace_id).map(String::as_str),
                    )
                    .await;
                    if outcome.purge_had_a_failure() {
                        // The inline `warn!`s inside `purge_slack_workspace`
                        // already logged the specific cause — this is the
                        // "make it findable from THIS route's own logs, not
                        // just the shared module's" signal, so a failed purge
                        // (token rows ARE gone by this point) never reads as
                        // "watched-channel/seat purge complete" — see
                        // `PurgeOutcome::purge_had_a_failure`'s own doc comment.
                        tracing::warn!(
                            account_id = %user_ctx.account_id,
                            workspace_id = %workspace_id,
                            ?outcome,
                            connector_health = true,
                            "slack disconnect: watched-channel/seat purge FAILED (partially) — \
                             token/integration rows are already gone; stale destinations may \
                             resurrect on reconnect"
                        );
                    } else {
                        tracing::info!(
                            account_id = %user_ctx.account_id,
                            workspace_id = %workspace_id,
                            ?outcome,
                            "slack disconnect: watched-channel/seat purge complete"
                        );
                    }
                    // Connector compliance audit (m365 enterprise train PR
                    // 4.2): durability tier 3 (`record_or_queue`) — this
                    // purge is ALREADY best-effort by this module's own
                    // documented convention (a partial failure here does not
                    // roll back the disconnect), so its audit row inherits
                    // that same posture rather than the request-path hard
                    // bar the `disconnect` row above already satisfied.
                    crate::services::connector_audit::record_or_queue(
                        tenant_pool,
                        crate::services::connector_audit::AuditEntry {
                            account_id: user_ctx.account_id,
                            actor: crate::services::connector_audit::Actor::User(user_ctx.user_id),
                            action: "purge",
                            provider: Some("slack".to_string()),
                            subject: Some(workspace_id.clone()),
                            detail: Some(json!({
                                "watched_channels_deleted": outcome.watched_channels_deleted,
                                "seats_revoked": outcome.seats_revoked,
                                "channels_left": outcome.channels_left,
                                "had_failure": outcome.purge_had_a_failure(),
                            })),
                        },
                    )
                    .await;
                }
            }
            None => tracing::warn!(
                account_id = %user_ctx.account_id,
                "slack disconnect: no tenant pool available — skipping watched-channel/seat \
                 purge (the token/integration rows are already gone above)"
            ),
        }
        // S6: account-wide bot disconnect may be loss of the
        // cold-start-bot shared capability. Runs even when the tenant
        // pool is absent — Slack health rows live on control. Only when
        // this DELETE targeted AccountWide (`owner_user_id IS NULL`) — a
        // member's `slack_user` disconnect uses Member owner and must NOT
        // open this row. The disconnecting admin is recorded in detail,
        // not as backing_user_id (the lost backer is the account-wide bot).
        if matches!(delete_owner, crate::connectors::TokenOwner::AccountWide) {
            if let Err(e) = crate::services::connector_health::on_backer_lost(
                &pool,
                tenant.pool.as_ref(),
                user_ctx.account_id,
                None,
                "slack",
                crate::services::connector_health::BackerLostCause::Disconnect,
                None,
                Some(json!({ "disconnected_by": user_ctx.user_id })),
            )
            .await
            {
                tracing::warn!(
                    error = %e,
                    account_id = %user_ctx.account_id,
                    "slack disconnect: on_backer_lost for cold-start bot failed — \
                     disconnect itself succeeded"
                );
            }
        }
    }

    // ── Shared disconnect purge — Microsoft (m365 Train 3 PR 3.7; owner scope
    // fixed CS-452) ──────────────────────────────────────────────────────────
    // Same "runs after commit, writes to the content-plane tenant pool" shape
    // as the Slack purge just above — see `connectors::microsoft_disconnect`'s
    // own header for the full contract (what it purges, what it deliberately
    // does not, and why a surviving `graph_subscriptions` row is the failure
    // that matters most). `delete_owner` (CS-452), not a hardcoded
    // `TokenOwner::AccountWide`: this call site used to pass `AccountWide`
    // unconditionally, on the stale premise that "every Microsoft `oauth_
    // tokens` row this codebase writes today IS account-wide (per-member
    // Microsoft OAuth is Train 3.2, not yet built)". That stopped being true
    // once m365 enterprise train PR 3.3 landed `org_microsoft_teams_
    // enabled_for` and Microsoft's own org/provider callback gate (`callback`'s
    // own doc comment) — an org member connecting Microsoft writes a REAL
    // `TokenOwner::Member(uid)` row today, so the old hardcoded `AccountWide`
    // purge scope meant one org member disconnecting Microsoft wiped EVERY
    // member's Outlook/Teams watches and subscriptions (the `oauth_tokens`
    // DELETE above was already owner-exact by then via `resolve_delete_
    // token_owner` — only this purge call still had the account-wide blast
    // radius). `delete_owner` is the SAME resolved value the `oauth_tokens`
    // DELETE above just used, so the two stay in lockstep by construction:
    // `AccountWide` on the personal plane (and org-plane Microsoft
    // reachable-but-off, today's overwhelmingly common case) purges
    // everything, exactly as before; `Member(uid)` on the org plane purges
    // only that member's own rows. Gated on `!removed_rows` having been empty
    // (the earlier `NOT_FOUND` return) via simply being provider-scoped: this
    // only runs when a `microsoft` row was actually deleted above.
    if provider == "microsoft" {
        match &tenant.pool {
            Some(tenant_pool) => {
                let outcome = crate::connectors::microsoft_disconnect::purge_microsoft_connection(
                    &state.http_client,
                    tenant_pool,
                    user_ctx.account_id,
                    delete_owner,
                    microsoft_access_token.as_deref(),
                )
                .await;
                if outcome.purge_had_a_failure() {
                    tracing::warn!(
                        account_id = %user_ctx.account_id,
                        ?outcome,
                        connector_health = true,
                        "microsoft disconnect: watched-resource/subscription purge FAILED \
                         (partially) — token rows are already gone; stale watches may keep \
                         ingesting or resurrect on reconnect"
                    );
                } else {
                    tracing::info!(
                        account_id = %user_ctx.account_id,
                        ?outcome,
                        "microsoft disconnect: watched-resource/subscription purge complete"
                    );
                }
                // Connector compliance audit (m365 enterprise train PR
                // 4.2): same durability tier 3 (`record_or_queue`) posture
                // as the Slack purge above, and for the identical reason —
                // this purge is already best-effort by convention.
                crate::services::connector_audit::record_or_queue(
                    tenant_pool,
                    crate::services::connector_audit::AuditEntry {
                        account_id: user_ctx.account_id,
                        actor: crate::services::connector_audit::Actor::User(user_ctx.user_id),
                        action: "purge",
                        provider: Some("microsoft".to_string()),
                        subject: None,
                        detail: Some(json!({
                            "watched_conversations_deleted": outcome.watched_conversations_deleted,
                            "teams_watched_deleted": outcome.teams_watched_deleted,
                            "subscriptions_deleted": outcome.subscriptions_deleted,
                            "had_failure": outcome.purge_had_a_failure(),
                        })),
                    },
                )
                .await;
            }
            None => tracing::warn!(
                account_id = %user_ctx.account_id,
                "microsoft disconnect: no tenant pool available — skipping watched-resource/\
                 subscription purge (the token rows are already gone above)"
            ),
        }
        // Doorbells this member owns are re-elected here. The Graph call, if
        // any, already ran above before the token row was deleted.
        // `on_backer_lost` drops only this member's watcher rows.
        if let Err(e) = crate::services::connector_health::on_backer_lost(
            &pool,
            tenant.pool.as_ref(),
            user_ctx.account_id,
            Some(user_ctx.user_id),
            "microsoft",
            crate::services::connector_health::BackerLostCause::Disconnect,
            None,
            None,
        )
        .await
        {
            tracing::warn!(
                error = %e,
                account_id = %user_ctx.account_id,
                "microsoft disconnect: on_backer_lost for teams doorbells failed — \
                 disconnect itself succeeded"
            );
        }
    }

    // ── Shared disconnect purge — Google (gsuite Train G2 PR 5, debt) ──────────
    // Same "runs after commit, writes to the content-plane tenant pool" shape
    // as the Slack/Microsoft purges above — see `connectors::google_disconnect`'s
    // own header for the full contract (what it purges, what it deliberately
    // does not, why no best-effort upstream `users.stop`/`channels.stop` call
    // is attempted, why `purge_not_after` is required here but not for
    // Slack/Microsoft, and the evidence-backed disposition on why a failed
    // purge here is not retried, matching the Slack/Microsoft precedent just
    // above). Two provider keys reach here, each a different
    // `Surface`/`TokenOwner` pair: `"google_gmail"` is per-member
    // (`Surface::Gmail`/`TokenOwner::Member` — the `oauth_tokens` DELETE
    // above is already owner-scoped to `user_ctx.user_id` for this provider,
    // so this purge must be scoped identically or it would remove a
    // DIFFERENT member's still-live Gmail watch/threads); `"google"` is
    // Calendar's grandfathered account-wide shape (`Surface::Calendar`/
    // `TokenOwner::AccountWide`, same reasoning `connectors::google::Surface`'s
    // own doc comment gives for why Calendar has no per-member storage yet).
    // `"google_chat"` joins them as of gsuite Train G4 PR 4 (design doc
    // §1.8): per-member exactly like Gmail (`Surface::Chat`/
    // `TokenOwner::Member`), and reaching the purge is what makes a Chat
    // disconnect actually stop ingest — its `surface='chat'`
    // `google_watch_channels` row and its participation in every watched
    // space are otherwise left behind for the renewal worker to keep
    // spinning on. `"google_meet"` joins them as of gsuite Train G3 PR 2:
    // per-member exactly like Gmail/Chat (`Surface::Meet`/
    // `TokenOwner::Member`), reaching a DIFFERENT purge step
    // (`purge_google_connection`'s step 4, `google_meet_sync_state`) since
    // Meet has no `google_watch_channels` row to begin with (pull-only in
    // this PR — see `connectors::google_meet`'s own module doc).
    if provider == "google_gmail"
        || provider == "google"
        || provider == "google_chat"
        || provider == "google_meet"
    {
        let (surface, owner) = match provider.as_str() {
            "google_gmail" => (
                crate::connectors::google::Surface::Gmail,
                TokenOwner::Member(user_ctx.user_id),
            ),
            "google_chat" => (
                crate::connectors::google::Surface::Chat,
                TokenOwner::Member(user_ctx.user_id),
            ),
            "google_meet" => (
                crate::connectors::google::Surface::Meet,
                TokenOwner::Member(user_ctx.user_id),
            ),
            // Calendar: plane-dependent owner, resolved by the SAME function
            // the `oauth_tokens` DELETE above used (`delete_owner`), so the
            // purge below is scoped to exactly the row that was removed.
            _ => (
                crate::connectors::google::Surface::Calendar,
                delete_owner,
            ),
        };
        match &tenant.pool {
            Some(tenant_pool) => {
                // `tenant_purge_not_after`/`control_purge_not_after` are each
                // sourced from THAT SAME pool's OWN clock (`SELECT now()`),
                // not `Utc::now()` in this API process (CodeRabbit round 2 on
                // PR #690, Major, confirmed: an app-process cutoff compared
                // against Postgres-`now()`-stamped `updated_at`/
                // `last_ingested_at` columns is only correct if the two
                // clocks agree; a host running behind the DB's clock would
                // make a genuinely stale row fail `updated_at <=
                // purge_not_after`, so the DELETE matches 0 rows and this
                // logs "purge complete" while the row survives). gsuite Train
                // G2 PR 7: `google_watch_channels` (surface `'gmail'`) now
                // lives on CONTROL, not `tenant_pool` — `pool` (this
                // function's own control-plane handle, resolved above) is
                // that pool, so its clock closes the identical race for that
                // table's own `updated_at`. Either query failing skips the
                // WHOLE purge with a warning, same posture as the `None`
                // branch below — the disconnect itself already succeeded
                // above; this only leaves stale watch state to self-heal on
                // reconnect or lapse naturally (see
                // `connectors::google_disconnect`'s own module doc, "No
                // durable retry on purge failure").
                let (control_now,tenant_now) = google_purge_clocks.expect("Google cleanup captured its clocks before token removal");
                match (control_now, tenant_now) {
                    (Ok(control_purge_not_after), Ok(tenant_purge_not_after)) => {
                        let outcome =
                            crate::connectors::google_disconnect::purge_google_connection(
                                &pool,
                                tenant_pool,
                                user_ctx.account_id,
                                surface,
                                owner,
                                control_purge_not_after,
                                tenant_purge_not_after,
                            )
                            .await;
                        if outcome.purge_had_a_failure() {
                            tracing::warn!(
                                account_id = %user_ctx.account_id,
                                provider = %provider,
                                ?outcome,
                                connector_health = true,
                                "google disconnect: watch-channel/thread purge FAILED (partially) — \
                                 token rows are already gone; a stale watch may keep the renewal worker \
                                 spinning until it lapses naturally, or resurrect on reconnect"
                            );
                        } else {
                            tracing::info!(
                                account_id = %user_ctx.account_id,
                                provider = %provider,
                                ?outcome,
                                "google disconnect: watch-channel/thread purge complete"
                            );
                        }
                    }
                    (control_now, tenant_now) => {
                        let e = control_now
                            .err()
                            .or(tenant_now.err())
                            .expect("at least one side failed");
                        tracing::warn!(
                            error = %e,
                            account_id = %user_ctx.account_id,
                            provider = %provider,
                            connector_health = true,
                            "google disconnect: failed to read a database clock — skipping \
                             watch-channel/thread purge entirely rather than risk deleting a row a \
                             concurrent reconnect just refreshed (the token rows are already gone above)"
                        );
                    }
                }
            }
            None => tracing::warn!(
                account_id = %user_ctx.account_id,
                provider = %provider,
                "google disconnect: no tenant pool available — skipping watch-channel/thread \
                 purge (the token rows are already gone above)"
            ),
        }
    }

    // ── GitHub: pause, never purge (docs/connectors/github.md §3.8) ────────
    // Deliberately NOT shaped like the Slack/Microsoft/Google purges above —
    // this connector's whole contract is that a disconnect never deletes
    // anything (`connectors::github_disconnect`'s own module doc). The
    // stored token is owner-scoped, so pause only that member's watch rows —
    // another member's still-valid connection keeps its repos.
    if provider == "github" {
        match &tenant.pool {
            Some(tenant_pool) => {
                match crate::connectors::github_disconnect::pause_for_member(
                    tenant_pool,
                    user_ctx.account_id,
                    user_ctx.user_id,
                )
                .await
                {
                    Ok(paused) => {
                        tracing::info!(account_id = %user_ctx.account_id, paused, "github disconnect: paused watched repos")
                    }
                    Err(e) => tracing::warn!(
                        error = %e,
                        account_id = %user_ctx.account_id,
                        connector_health = true,
                        "github disconnect: failed to pause watched repos — token row is already \
                         gone; repos may keep resolving through a now-invalid credential until the \
                         next backfill attempt fails closed"
                    ),
                }
            }
            None => tracing::warn!(
                account_id = %user_ctx.account_id,
                "github disconnect: no tenant pool available — skipping watched-repo pause \
                 (the token row is already gone above)"
            ),
        }
    }

    (StatusCode::OK, Json(json!({"status": "disconnected"}))).into_response()
}

// ---------------------------------------------------------------------------
// DELETE /v1/oauth/connections/slack_user — member disconnect (CS-454)
// ---------------------------------------------------------------------------

/// Disconnect the CALLING member's own `slack_user` row — [`delete_connection`]'s
/// early branch for `provider == "slack_user"` (see that function's own doc
/// comment for why this is a separate function rather than a third arm on
/// its shared multi-provider DELETE).
///
/// **Owner-exact, never `owner_user_id IS NULL OR owner_user_id = $n`.**
/// A `slack_user` row is ALWAYS `TokenOwner::Member` (`resolve_slack_token_
/// target` never writes one any other way), so there is no account-wide row
/// of this provider for the `OR` half of that pattern to ever legitimately
/// match — including it here would just be dead code wearing the shape of a
/// real guard. This is also deliberately narrow so it shares no line with
/// CS-452's concurrent removal of that exact fallback from [`delete_connection`]'s
/// own query.
///
/// **Revokes at Slack BEFORE deleting locally.** Best-effort — Slack
/// returning `token_revoked`/`invalid_auth` (already revoked/expired) is not
/// a reason to fail a disconnect the caller explicitly asked for, and
/// neither is a network failure; either way the local rows are removed
/// regardless, same "provider-side revocation is attempted on a best-effort
/// basis (failure does not affect the response)" posture [`delete_connection`]'s
/// own doc comment states for itself. Revoking BEFORE the delete (not after,
/// the order [`delete_connection`] uses for its own post-commit Slack purge)
/// is safe to do here specifically because there is no shared bot asset
/// downstream depending on this row surviving a moment longer — unlike the
/// bot row's purge, which needs the plaintext token AFTER the row is gone
/// (see that function's own comment), this member row's only consumer is
/// this disconnect itself.
///
/// No admin gate — self-service, same as the connect half (item 46a's own
/// "lift the admin gate for this user-scope-only connect").
///
/// `tenant` is the request's binding-gate decision so the DEK plane matches
/// the one `store_token_bundle_tx` encrypted this row under
/// (`DekPlane::from_decision`, same as [`delete_connection`]). A bare
/// `DekPlane::consumer` cannot unwrap an org member token stored under
/// tenant `account_keys`.
pub(crate) async fn delete_slack_member_connection(
    state: &AppState,
    tenant: &Tenant,
    user_ctx: &UserContext,
) -> Response {
    let pool = match state.control_db() {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };

    // Owner-exact lookup (never `OR owner_user_id IS NULL` — see this
    // function's own doc comment) for the encrypted token, so it can be
    // revoked at Slack BEFORE either row is deleted. Explicit WHERE clauses
    // do not set the strict oauth_tokens RLS policy's account context.
    // Close this scoped read before provider latency, then open the separate
    // atomic delete transaction below.
    let mut read_tx = match crate::tenancy::scope::begin_account_scoped(&pool, user_ctx.account_id).await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %user_ctx.account_id,
                "slack member disconnect: failed to open scoped read tx"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    let row: Option<(String, Option<String>, Vec<u8>)> = match sqlx::query_as(
        "SELECT id::text, provider_workspace_id, access_token FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'slack_user' AND owner_user_id = $2",
    )
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .fetch_optional(&mut *read_tx)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %user_ctx.account_id,
                "slack member disconnect: failed to look up oauth_tokens row"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };
    if let Err(e) = read_tx.commit().await {
        tracing::warn!(
            error = %e,
            account_id = %user_ctx.account_id,
            "slack member disconnect: failed to close scoped read tx"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response();
    }

    let Some((_row_id, workspace_id, encrypted_access_token)) = row else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "connection not found"})),
        )
            .into_response();
    };

    // Decrypt and revoke at Slack, best-effort — see this function's own doc
    // comment for why a failure here never blocks the local delete below.
    // Org member tokens are encrypted under the tenant `account_keys` DEK
    // (`resolve_callback_dek_plane`); `from_decision` is the same plane
    // [`delete_connection`] uses. `DekPlane::consumer` would miss that key
    // (or unwrap a decoy control-plane DEK) and skip `auth.revoke`.
    let dek_plane = DekPlane::from_decision(tenant.decision.as_ref(), &pool);
    match dek_plane.resolve(user_ctx.account_id).await {
        Ok(Some(dek)) => match decrypt_token(&dek, &encrypted_access_token) {
            Ok(plaintext) => {
                if let Err(e) =
                    crate::connectors::slack::revoke_member_token(&state.http_client, &plaintext)
                        .await
                {
                    tracing::info!(
                        error = %e,
                        account_id = %user_ctx.account_id,
                        "slack member disconnect: auth.revoke failed (non-fatal) — proceeding \
                         with local delete regardless"
                    );
                } else {
                    tracing::info!(
                        account_id = %user_ctx.account_id,
                        "slack member disconnect: auth.revoke succeeded"
                    );
                }
            }
            Err(e) => tracing::warn!(
                error = %e,
                account_id = %user_ctx.account_id,
                "slack member disconnect: failed to decrypt the stored token — skipping \
                 auth.revoke (non-fatal), proceeding with local delete"
            ),
        },
        Ok(None) => tracing::warn!(
            account_id = %user_ctx.account_id,
            "slack member disconnect: account has no dek_wrapped provisioned — skipping \
             auth.revoke (non-fatal), proceeding with local delete"
        ),
        Err(e) => tracing::warn!(
            error = %e,
            account_id = %user_ctx.account_id,
            "slack member disconnect: failed to resolve account DEK — skipping auth.revoke \
             (non-fatal), proceeding with local delete"
        ),
    }

    // Owner-exact credential, routing, and channel-index deletes in one
    // transaction. `connector_installs`
    // is keyed `provider = 'slack'` (never `'slack_user'` — see `callback`'s
    // own `connector_installs_provider` comment) with `owner_user_id = $2`,
    // the per-owner row `record_connector_install` wrote alongside this
    // member's `oauth_tokens` row — deleting it here, and ONLY it (never the
    // account-wide `owner_user_id IS NULL` row the bot install owns), is what
    // keeps a member's disconnect from touching the shared workspace route.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(&pool, user_ctx.account_id).await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user_ctx.account_id, "slack member disconnect: failed to open scoped tx");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    let deleted = sqlx::query(
        "DELETE FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'slack_user' AND owner_user_id = $2",
    )
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .execute(&mut *tx)
    .await;
    match deleted {
        Ok(r) if r.rows_affected() > 0 => {}
        Ok(_) => {
            // Deleted by a concurrent request between the SELECT above and
            // here — same "not found" answer a genuinely-never-connected
            // caller gets, not an error.
            tx.rollback().await.ok();
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "connection not found"})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, account_id = %user_ctx.account_id, "slack member disconnect: failed to delete oauth_tokens row");
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    if let Some(workspace_id) = &workspace_id {
        if let Err(e) = sqlx::query(
            "DELETE FROM connector_installs \
             WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id = $2 \
               AND owner_user_id = $3",
        )
        .bind(user_ctx.account_id)
        .bind(workspace_id)
        .bind(user_ctx.user_id)
        .execute(&mut *tx)
        .await
        {
            tracing::error!(
                error = %e,
                account_id = %user_ctx.account_id,
                "slack member disconnect: failed to delete connector_installs row — aborting so \
                 the caller retries rather than leaving oauth_tokens and connector_installs \
                 disagreeing"
            );
            tx.rollback().await.ok();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    }

    if let Err(e) = sqlx::query(
        "DELETE FROM slack_member_channels WHERE account_id=$1 AND user_id=$2",
    )
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .execute(&mut *tx)
    .await
    {
        tracing::error!(error = %e, account_id = %user_ctx.account_id,
            "slack member disconnect: failed to clear member channel index");
        tx.rollback().await.ok();
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"})))
            .into_response();
    }

    if let Err(e) = crate::tenancy::scope::set_tenant_user(&mut tx, user_ctx.user_id).await {
        tracing::error!(error = %e, account_id = %user_ctx.account_id, "slack member disconnect: failed to set app.user_id for connector_audit_log write");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response();
    }
    if let Err(e) = crate::services::connector_audit::record_in_tx(
        &mut tx,
        crate::services::connector_audit::AuditEntry {
            account_id: user_ctx.account_id,
            actor: crate::services::connector_audit::Actor::User(user_ctx.user_id),
            action: "disconnect",
            provider: Some("slack_user".to_string()),
            subject: workspace_id.clone(),
            detail: None,
        },
    )
    .await
    {
        tracing::error!(
            error = %e,
            account_id = %user_ctx.account_id,
            "slack member disconnect: failed to write connector_audit_log 'disconnect' row — \
             failing the disconnect rather than committing an unaudited one"
        );
        tx.rollback().await.ok();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to persist audit record"})),
        )
            .into_response();
    }

    if let Err(e) = tx.commit().await {
        tracing::warn!(error = %e, account_id = %user_ctx.account_id, "slack member disconnect: failed to commit");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response();
    }
    if let Err(e)=crate::read_models::dirty::connector_changed(state,tenant,user_ctx.account_id,user_ctx.user_id).await {
        tracing::error!(error=%e,"Slack member revoke read-model invalidation failed");
        return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"read model invalidation failed"}))).into_response();
    }


    tracing::info!(account_id = %user_ctx.account_id, "slack member connection revoked");
    (StatusCode::OK, Json(json!({"status": "disconnected"}))).into_response()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test-only: the one lock every test that mutates an `ORG_GOOGLE_*_ENABLED`
/// (or `GOOGLE_CALENDAR_CLIENT_ID`) env var must hold — see the `use` in
/// `mod tests` for the full rationale.
#[cfg(test)]
pub(crate) static GOOGLE_SURFACE_ENV_LOCK: tokio::sync::Mutex<()> =
    tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::{delete, get},
        Router,
    };
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    // The web's first-run checklist keys "connectors are off in this org" on
    // this code, not on the sentence; changing either half breaks it.
    #[tokio::test]
    async fn org_management_refusal_carries_the_connectors_off_code() {
        let resp = org_management_refused_response();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "org_connectors_off");
        assert!(body["error"]
            .as_str()
            .is_some_and(|e| e.starts_with("connector authorization is available in your personal workspace only")));
    }

    fn connections_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/oauth/connections", get(list_connections))
            .route(
                "/v1/oauth/connections/{provider}",
                delete(delete_connection),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    // ── Connections list requires auth ────────────────────────────────────────

    #[tokio::test]
    async fn list_connections_requires_auth() {
        let router = connections_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/oauth/connections")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Connections list with bearer + no DB → 503 ────────────────────────────

    #[tokio::test]
    async fn list_connections_no_db_returns_503() {
        let router = connections_router(test_state());
        let req = HttpRequest::builder()
            .uri("/v1/oauth/connections")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // require_auth middleware hits 503 before handler (no db).
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Delete connection requires auth ───────────────────────────────────────

    #[tokio::test]
    async fn delete_connection_requires_auth() {
        let router = connections_router(test_state());
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri("/v1/oauth/connections/slack")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Authorize endpoint: missing SLACK_CLIENT_ID → 503 ────────────────────

    #[tokio::test]
    async fn authorize_missing_client_id_returns_503() {
        // Ensure env var is absent for this test.
        unsafe {
            std::env::remove_var("SLACK_CLIENT_ID");
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/slack/authorize")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Authorize endpoint: unknown provider → 404 ───────────────────────────

    #[tokio::test]
    async fn authorize_unknown_provider_returns_404() {
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(state);

        // GitHub connector PR A registered "github" (`connectors::lookup`),
        // so it is no longer a valid "unknown provider" example — same fix
        // as `connectors::tests::lookup_unknown_provider_is_none`'s own
        // switch to "bitbucket".
        let req = HttpRequest::builder()
            .uri("/v1/oauth/bitbucket/authorize")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── Authorize endpoint: Google missing client id → 503 ──────────────────
    // Google is now implemented (cs-uxp4a-calendar), so an unconfigured Google
    // connector reports NotConfigured (503), not NotImplemented (501).

    /// Serializes every test in this module that mutates `GOOGLE_CALENDAR_
    /// CLIENT_ID` or `ORG_GOOGLE_GMAIL_ENABLED` — `cargo test` runs
    /// `#[tokio::test]`s concurrently within one process, so unguarded
    /// `set_var`/`remove_var` on these process-global env vars could race a
    /// concurrent test's read of the same var. Same pattern (and same
    /// rationale) as this module's own `OAUTH_STATE_SECRET_ENV_LOCK` above
    /// and `workers::huddle`'s/`routes::slack_events`'s identically-shaped
    /// `*_ENV_LOCK` statics (CodeRabbit #669 finding).
    ///
    /// Re-exported from the crate-level [`GOOGLE_SURFACE_ENV_LOCK`] below so
    /// `routes::connectors`' Calendar-flag tests serialize against THIS
    /// module's Calendar-flag tests (one process-global env var, two test
    /// modules).
    use super::GOOGLE_SURFACE_ENV_LOCK;

    #[tokio::test]
    async fn authorize_google_missing_client_id_returns_503() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        unsafe {
            std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID");
        }

        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        drop(_guard);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Authorize endpoint: Google surface gate (gsuite Train G1 PR 1) ───────

    #[tokio::test]
    async fn authorize_google_default_surface_is_calendar() {
        // No `?surface=` — must behave exactly as before this PR: Calendar,
        // ungated, missing-client-id → 503 (never 403).
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        unsafe {
            std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID");
        }
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        drop(_guard);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn authorize_google_unknown_surface_returns_400() {
        // Touches no env var — no lock needed.
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize?surface=drive")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn authorize_google_gmail_surface_refused_when_flag_off() {
        // ORG_GOOGLE_GMAIL_ENABLED unset (default off) and no state token, so
        // there is nothing to override against — the raw env default (false)
        // applies, and Gmail must be refused BEFORE any client-id or
        // implemented-surface check.
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        unsafe {
            std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED");
        }
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize?surface=gmail")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        drop(_guard);
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── gsuite Train G3 PR 2: Meet's 501 gate lifts ──────────────────────────
    //
    // Formerly `authorize_google_meet_surface_not_implemented_even_when_flag_on`
    // (CodeRabbit #669 finding): with Chat's own gate lifted in G4.1, Meet was
    // the sole surviving surface still refused with 501 once its flag was on.
    // This PR lifts it too — real ingest (`workers::google_meet_sweep`) now
    // reads a stored `google_meet` token from day one, so there is no longer
    // an "implemented but nothing reads it" state to protect against. Meet's
    // two tests below mirror Chat's own gate-lift pair exactly.

    #[tokio::test]
    async fn authorize_google_meet_surface_flag_on_no_state_is_refused_not_a_redirect() {
        // Mirrors `authorize_google_chat_surface_flag_on_no_state_is_refused_
        // not_a_redirect` exactly: Meet is now `is_implemented() == true`, so
        // with the flag on, a client id configured, AND `OAUTH_STATE_SECRET`
        // configured (so the ONLY thing missing is the caller's `?state=`),
        // this must NOT fall through to a 501 or a real redirect —
        // `state_val` defaults to a freshly-generated random UUID (not a
        // JWT), and `require_verified_surface_state` must refuse it with 400
        // (no verifiable signed state, no silent `AccountWide` fallback —
        // G2.1's rule, since `Surface::Meet::token_owner_for` is always
        // `Member`, same as Gmail/Chat's).
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_MEET_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        // SAFETY: `_g1`/`_g2` (held above, `GOOGLE_SURFACE_ENV_LOCK` and
        // `OAUTH_STATE_SECRET_ENV_LOCK`) together serialize every test that
        // mutates `ORG_GOOGLE_MEET_ENABLED`/`GOOGLE_CALENDAR_CLIENT_ID`/
        // `OAUTH_STATE_SECRET`, so no other test can observe or race these
        // process-global env writes.
        unsafe {
            std::env::set_var("ORG_GOOGLE_MEET_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "meet-authorize-test-secret-nostatezzz",
            );
        }
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize?surface=meet")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_MEET_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_MEET_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn authorize_google_meet_matching_verified_state_redirects() {
        // The one success path for Meet, mirroring Chat's own: flag on,
        // client id configured, and a state that is itself validly signed
        // AND declares BOTH a connecting user id and exactly this surface —
        // must redirect to Google (never a 400/403/501/503), and the
        // redirect URL must carry Meet's frozen scope (unchanged by this
        // PR — see `connectors::google::Surface::Meet::data_scope`'s own
        // doc for the scope-question research this PR resolved without
        // widening it).
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_MEET_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "meet-authorize-test-secret-klmnopqrstuv";
        // SAFETY: `_g1`/`_g2` (held above, `GOOGLE_SURFACE_ENV_LOCK` and
        // `OAUTH_STATE_SECRET_ENV_LOCK`) together serialize every test that
        // mutates `ORG_GOOGLE_MEET_ENABLED`/`GOOGLE_CALENDAR_CLIENT_ID`/
        // `OAUTH_STATE_SECRET`, so no other test can observe or race these
        // process-global env writes.
        unsafe {
            std::env::set_var("ORG_GOOGLE_MEET_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var("OAUTH_STATE_SECRET", secret);
        }
        let token = oauth_state::sign_state(
            secret,
            Uuid::new_v4(),
            Utc::now().timestamp(),
            "meet-authorize-test-nonce-3",
            Some(Uuid::new_v4()),
            Some("meet"),
        )
        .unwrap();

        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());
        let req = HttpRequest::builder()
            .uri(format!(
                "/v1/oauth/google/authorize?surface=meet&state={token}"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_MEET_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_MEET_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            location.contains("meetings.space.readonly"),
            "the frozen, unwidened scope must be on the actual redirect URL: {location}"
        );
    }

    // ── gsuite Train G4 PR 1 ("G4.1"): Chat's 501 gate lifts ─────────────────

    #[tokio::test]
    async fn authorize_google_chat_surface_refused_when_flag_off() {
        // Same shape as Gmail's flag-off test above: with the flag off (and
        // no state token to override against), the raw env default (false)
        // applies and Chat is refused BEFORE any client-id or
        // implemented-surface check — 403, not the 501 a not-yet-implemented
        // surface would report.
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        // SAFETY: `_guard` (held above, `GOOGLE_SURFACE_ENV_LOCK`) serializes
        // every test in this module that mutates `ORG_GOOGLE_CHAT_ENABLED`,
        // so no other test can observe or race this process-global env write.
        unsafe {
            std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED");
        }
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize?surface=chat")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        drop(_guard);
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn authorize_google_chat_surface_flag_on_no_state_is_refused_not_a_redirect() {
        // Mirrors `authorize_google_gmail_no_state_is_refused_not_a_redirect`
        // exactly: Chat is now `is_implemented() == true`, so with the flag
        // on, a client id configured, AND `OAUTH_STATE_SECRET` configured
        // (so the ONLY thing missing is the caller's `?state=`), this must
        // NOT fall through to a 501 or a real redirect — `state_val`
        // defaults to a freshly-generated random UUID (not a JWT), and
        // `require_verified_surface_state` must refuse it with 400 (no
        // verifiable signed state, no silent `AccountWide` fallback — G2.1's
        // rule, extended to Chat by this PR since `Surface::Chat::
        // token_owner_for` is always `Member`, same as Gmail's).
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        // SAFETY: `_g1`/`_g2` (held above, `GOOGLE_SURFACE_ENV_LOCK` and
        // `OAUTH_STATE_SECRET_ENV_LOCK`) together serialize every test that
        // mutates `ORG_GOOGLE_CHAT_ENABLED`/`GOOGLE_CALENDAR_CLIENT_ID`/
        // `OAUTH_STATE_SECRET`, so no other test can observe or race these
        // process-global env writes.
        unsafe {
            std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "chat-authorize-test-secret-nostatezzz",
            );
        }
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize?surface=chat")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn authorize_google_chat_matching_verified_state_redirects() {
        // The one success path for Chat, mirroring Gmail's own: flag on,
        // client id configured, and a state that is itself validly signed
        // AND declares BOTH a connecting user id and exactly this surface —
        // must redirect to Google (never a 400/403/501/503), and the
        // redirect URL must carry the AMENDED scope set (this PR's whole
        // point) — including the newly added `chat.memberships.readonly`,
        // not just the two scopes the gate used to be frozen on.
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "chat-authorize-test-secret-klmnopqrstuv";
        // SAFETY: `_g1`/`_g2` (held above, `GOOGLE_SURFACE_ENV_LOCK` and
        // `OAUTH_STATE_SECRET_ENV_LOCK`) together serialize every test that
        // mutates `ORG_GOOGLE_CHAT_ENABLED`/`GOOGLE_CALENDAR_CLIENT_ID`/
        // `OAUTH_STATE_SECRET`, so no other test can observe or race these
        // process-global env writes.
        unsafe {
            std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var("OAUTH_STATE_SECRET", secret);
        }
        let token = oauth_state::sign_state(
            secret,
            Uuid::new_v4(),
            Utc::now().timestamp(),
            "chat-authorize-test-nonce-3",
            Some(Uuid::new_v4()),
            Some("chat"),
        )
        .unwrap();

        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());
        let req = HttpRequest::builder()
            .uri(format!(
                "/v1/oauth/google/authorize?surface=chat&state={token}"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(location.contains("chat.spaces.readonly"), "{location}");
        assert!(location.contains("chat.messages.readonly"), "{location}");
        assert!(
            location.contains("chat.memberships.readonly"),
            "the amended scope must be on the actual redirect URL: {location}"
        );
    }

    // ── Gmail 501 gate lift (gsuite Train G2 PR 1, item 4) ────────────────────

    #[tokio::test]
    async fn authorize_google_gmail_no_state_is_refused_not_a_redirect() {
        // Gmail is now `is_implemented() == true`, so with the flag on, a
        // client id configured, AND `OAUTH_STATE_SECRET` configured (so the
        // ONLY thing missing is the caller's `?state=`), this must NOT fall
        // through to a 501 or a real redirect — `state_val` defaults to a
        // freshly-generated random UUID (not a JWT), and
        // `require_verified_surface_state` must refuse it with 400 (item 4:
        // no verifiable signed state, no silent AccountWide fallback). Needs
        // BOTH env locks: without `OAUTH_STATE_SECRET` set at all, the
        // request would 503 (server misconfiguration) before ever reaching
        // the "is this state verifiable" check — see
        // `require_verified_surface_state`'s own doc on that distinction.
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        unsafe {
            std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "gmail-authorize-test-secret-nostatezz",
            );
        }
        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());

        let req = HttpRequest::builder()
            .uri("/v1/oauth/google/authorize?surface=gmail")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Serializes every test below that mutates `OAUTH_STATE_SECRET`
    /// TOGETHER WITH the Google surface env vars — these tests need both
    /// locks (a real signed state AND the flag/client-id gates), always
    /// acquired in this same order to avoid a lock-order deadlock with any
    /// other test that might one day need both.
    async fn lock_google_surface_and_state_secret_env() -> (
        tokio::sync::MutexGuard<'static, ()>,
        tokio::sync::MutexGuard<'static, ()>,
    ) {
        (
            GOOGLE_SURFACE_ENV_LOCK.lock().await,
            OAUTH_STATE_SECRET_ENV_LOCK.lock().await,
        )
    }

    #[tokio::test]
    async fn authorize_google_gmail_valid_state_missing_surface_claim_is_refused() {
        // A REAL, validly-signed state — but minted the old way, with no
        // `sfc`/`uid` claims (e.g. a plain `mint_state` call that never
        // requested `?surface=gmail`). Passing this off as a Gmail authorize
        // must still be refused: `require_verified_surface_state` requires
        // the state's OWN claims to already declare this exact surface, not
        // just any valid signature.
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "gmail-authorize-test-secret-0123456789";
        unsafe {
            std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var("OAUTH_STATE_SECRET", secret);
        }
        let token = oauth_state::sign_state(
            secret,
            Uuid::new_v4(),
            Utc::now().timestamp(),
            "gmail-authorize-test-nonce-1",
            None,
            None,
        )
        .unwrap();

        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());
        let req = HttpRequest::builder()
            .uri(format!(
                "/v1/oauth/google/authorize?surface=gmail&state={token}"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn authorize_google_gmail_state_declaring_a_different_surface_is_refused() {
        // The state is validly signed AND carries a `uid`, but its `sfc`
        // claim says "calendar", not "gmail" — a mismatch must refuse the
        // SAME as a missing claim, never silently proceed under the
        // requested surface instead of the state's own.
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "gmail-authorize-test-secret-abcdefghij";
        unsafe {
            std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var("OAUTH_STATE_SECRET", secret);
        }
        let token = oauth_state::sign_state(
            secret,
            Uuid::new_v4(),
            Utc::now().timestamp(),
            "gmail-authorize-test-nonce-2",
            Some(Uuid::new_v4()),
            Some("calendar"),
        )
        .unwrap();

        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());
        let req = HttpRequest::builder()
            .uri(format!(
                "/v1/oauth/google/authorize?surface=gmail&state={token}"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn authorize_google_gmail_matching_verified_state_redirects() {
        // The one success path: flag on, client id configured, and a state
        // that is itself validly signed AND declares BOTH a connecting user
        // id and exactly this surface — must redirect to Google (never a
        // 400/403/501/503).
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let prior_flag = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "gmail-authorize-test-secret-klmnopqrst";
        unsafe {
            std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
            std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
            std::env::set_var("OAUTH_STATE_SECRET", secret);
        }
        let token = oauth_state::sign_state(
            secret,
            Uuid::new_v4(),
            Utc::now().timestamp(),
            "gmail-authorize-test-nonce-3",
            Some(Uuid::new_v4()),
            Some("gmail"),
        )
        .unwrap();

        let router = Router::new()
            .route("/v1/oauth/{provider}/authorize", get(authorize))
            .with_state(test_state());
        let req = HttpRequest::builder()
            .uri(format!(
                "/v1/oauth/google/authorize?surface=gmail&state={token}"
            ))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        match prior_client_id {
            Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
            None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(location.contains("gmail.readonly"), "{location}");
    }

    // ── resolve_google_token_target (gsuite Train G2 PR 1, items 2/3) ─────────
    // Pure, DB-free — the exact resolution `callback` runs.

    #[test]
    fn resolve_google_token_target_defaults_to_calendar_when_surface_claim_absent() {
        let (owner, provider) = resolve_google_token_target(None, None, false).unwrap();
        assert_eq!(owner, TokenOwner::AccountWide);
        assert_eq!(provider, "google");
    }

    #[test]
    fn resolve_google_token_target_defaults_to_calendar_when_surface_claim_unparseable() {
        // A garbage `sfc` value must default the SAME way an absent one does
        // — never treated as "unknown surface, refuse" (that would be a new
        // failure mode `callback` never had before this PR).
        let (owner, provider) = resolve_google_token_target(Some("drive"), None, false).unwrap();
        assert_eq!(owner, TokenOwner::AccountWide);
        assert_eq!(provider, "google");
    }

    #[test]
    fn resolve_google_token_target_calendar_ignores_a_present_uid() {
        // Even if a state somehow carried a `uid` alongside an absent/
        // calendar `sfc`, Calendar stays AccountWide — frozen, per
        // `Surface::token_owner_for`.
        let uid = Uuid::new_v4();
        let (owner, provider) = resolve_google_token_target(Some("calendar"), Some(uid), false).unwrap();
        assert_eq!(owner, TokenOwner::AccountWide);
        assert_eq!(provider, "google");
    }

    #[test]
    fn resolve_google_token_target_gmail_with_uid_returns_member_owner_and_its_own_provider_key() {
        let uid = Uuid::new_v4();
        let (owner, provider) = resolve_google_token_target(Some("gmail"), Some(uid), false).unwrap();
        assert_eq!(owner, TokenOwner::Member(uid));
        assert_eq!(provider, "google_gmail");
    }

    #[test]
    fn resolve_google_token_target_gmail_without_uid_is_refused_not_a_fallback() {
        // Item 4's rule: a non-Calendar surface with no embedded connecting
        // user id is an `Err`, never a silent `TokenOwner::AccountWide`.
        assert_eq!(resolve_google_token_target(Some("gmail"), None, false), Err(()));
    }

    // gsuite Train G4 PR 1 ("G4.1"): the identical resolution already covers
    // Chat with zero code changes here — `resolve_google_token_target`'s own
    // `_ =>` arm was written generically against every non-Calendar
    // `Surface`, not Gmail specifically (design doc §1.11: "already correct
    // and needing nothing"). Pinned explicitly for Chat anyway, since this
    // function is the exact resolution `callback` performs and conformance
    // test 1 (cross-member token isolation) depends on it being right.

    #[test]
    fn resolve_google_token_target_chat_with_uid_returns_member_owner_and_its_own_provider_key() {
        let uid = Uuid::new_v4();
        let (owner, provider) = resolve_google_token_target(Some("chat"), Some(uid), false).unwrap();
        assert_eq!(owner, TokenOwner::Member(uid));
        assert_eq!(provider, "google_chat");
    }

    #[test]
    fn resolve_google_token_target_chat_without_uid_is_refused_not_a_fallback() {
        // Same no-fallback rule as Gmail: per-member storage needs a `uid`,
        // no silent `AccountWide` fallback.
        assert_eq!(resolve_google_token_target(Some("chat"), None, false), Err(()));
    }

    // ── resolve_microsoft_token_target (m365 enterprise train PR 3.2) ────────
    // Pure, DB-free — the exact resolution `callback` runs.

    #[test]
    fn resolve_microsoft_token_target_personal_plane_is_account_wide_even_with_a_uid() {
        // Personal plane stays AccountWide unconditionally (unlike Google
        // Gmail's always-Member shape) — a present connecting-user id does
        // not flip it, since every existing Microsoft read call site this
        // PR does NOT touch (graph_webhook, workflows, graph_subscriptions
        // worker, teams_ingest) still hardcodes AccountWide.
        let uid = Uuid::new_v4();
        assert_eq!(
            resolve_microsoft_token_target(false, Some(uid)),
            Ok(TokenOwner::AccountWide)
        );
        assert_eq!(
            resolve_microsoft_token_target(false, None),
            Ok(TokenOwner::AccountWide)
        );
    }

    #[test]
    fn resolve_microsoft_token_target_org_plane_with_uid_returns_member_owner() {
        let uid = Uuid::new_v4();
        assert_eq!(
            resolve_microsoft_token_target(true, Some(uid)),
            Ok(TokenOwner::Member(uid))
        );
    }

    #[test]
    fn resolve_microsoft_token_target_org_plane_without_uid_is_refused_not_a_fallback() {
        // Same no-fallback rule as Google's Gmail branch: an org-plane state
        // with no embedded connecting user id is an `Err`, never a silent
        // `TokenOwner::AccountWide`. Unreachable from any real flow today
        // (the org/provider gate above refuses org+microsoft before this
        // runs) — tested anyway as the defense-in-depth it is.
        assert_eq!(resolve_microsoft_token_target(true, None), Err(()));
    }

    // ── Callback: missing state → 400 ────────────────────────────────────────

    #[tokio::test]
    async fn callback_missing_state_returns_400() {
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/callback", get(callback))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/slack/callback?code=abc123")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── Callback: invalid state (not a UUID) → 400 ───────────────────────────

    #[tokio::test]
    async fn callback_invalid_state_returns_400() {
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/callback", get(callback))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/slack/callback?code=abc123&state=not-a-uuid")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── Callback: provider error param → 400 ─────────────────────────────────

    #[tokio::test]
    async fn callback_provider_error_returns_400() {
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/callback", get(callback))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/slack/callback?error=access_denied&state=00000000-0000-0000-0000-000000000000")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── Callback: valid UUID state + no DB → 503 ─────────────────────────────

    #[tokio::test]
    async fn callback_wellformed_state_no_db_returns_503() {
        // A structurally-valid state token (three dotted segments) passes the
        // cheap shape check; with no DB pool the handler returns 503 before the
        // cryptographic verification step runs. (CS-110: the shape check precedes
        // the pool guard; full signature verification follows it.)
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/callback", get(callback))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/slack/callback?code=abc123&state=aaa.bbb.ccc")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Callback: the return page for a connect started by the catalog (B2) ──

    /// Run the callback with `OAUTH_STATE_SECRET` set to `secret`, no database,
    /// and return its status and `Location`.
    async fn callback_with_secret(secret: &str, query: &str) -> (StatusCode, Option<String>, String) {
        let _guard = OAUTH_STATE_SECRET_ENV_LOCK.lock().await;
        let prior = std::env::var("OAUTH_STATE_SECRET").ok();
        // SAFETY: `OAUTH_STATE_SECRET_ENV_LOCK` serializes every test that writes it.
        unsafe { std::env::set_var("OAUTH_STATE_SECRET", secret) };
        let state = test_state();
        let web = state.personal_web_base_url();
        let router = Router::new()
            .route("/v1/oauth/{provider}/callback", get(callback))
            .with_state(state);
        let req = HttpRequest::builder()
            .uri(format!("/v1/oauth/slack/callback?{query}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        let location = resp
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        (resp.status(), location, web)
    }

    fn connect_state(secret: &str, ret: Option<&str>) -> String {
        let (account, member) = (Uuid::new_v4(), Uuid::new_v4());
        let now = Utc::now().timestamp();
        match ret {
            Some(ret) => oauth_state::sign_state_for_connect(
                secret, account, now, "n-ret", Some(member), None, ret,
            ),
            None => oauth_state::sign_state(secret, account, now, "n-plain", Some(member), None),
        }
        .unwrap()
    }

    #[tokio::test]
    async fn a_catalog_connect_that_fails_returns_to_the_app_with_a_code() {
        let secret = "catalog-callback-test-secret-0123456789";
        let token = connect_state(secret, Some("wizard"));
        // No database in this fixture: the old handler answers 503.
        let (status, location, web) =
            callback_with_secret(secret, &format!("code=abc&state={token}")).await;
        assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            location.as_deref(),
            Some(format!("{web}/connectors/return?provider=slack&result=error&code=unavailable").as_str())
        );

        // Cancelling at the provider is `denied`, even though the old handler
        // answers a bare 400 for it.
        let (status, location, web) =
            callback_with_secret(secret, &format!("error=access_denied&state={token}")).await;
        assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            location.as_deref(),
            Some(format!("{web}/connectors/return?provider=slack&result=error&code=denied").as_str())
        );
    }

    #[tokio::test]
    async fn the_older_uis_callbacks_are_answered_exactly_as_before() {
        let secret = "catalog-callback-test-secret-0123456789";
        // A state without `ret`: the provider's error is still a bare 400.
        let plain = connect_state(secret, None);
        let (status, location, _) =
            callback_with_secret(secret, &format!("error=access_denied&state={plain}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(location, None);
        let (status, location, _) =
            callback_with_secret(secret, &format!("code=abc&state={plain}")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(location, None);

        // A `ret` this server did not sign is not believed: it cannot make the
        // callback redirect anywhere.
        let forged = connect_state("some-other-secret-aaaaaaaaaaaaaaaaaaaa", Some("wizard"));
        let (status, location, _) =
            callback_with_secret(secret, &format!("error=access_denied&state={forged}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(location, None);

        // A signed `ret` that is not one of the two targets is ignored too.
        let odd = connect_state(secret, Some("https://evil.example"));
        let (status, location, _) =
            callback_with_secret(secret, &format!("error=access_denied&state={odd}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(location, None);
    }

    // ── admin_consent: state must be a signed token, no raw-UUID fallback ────
    // (CS-110 follow-up, foundation train PR 1.4)

    #[tokio::test]
    async fn admin_consent_missing_state_returns_400() {
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/admin_consent", get(admin_consent))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/slack/admin_consent")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn admin_consent_raw_uuid_state_returns_400() {
        // A client-supplied raw UUID — the exact shape this handler used to
        // default to when `state` was absent — must be rejected structurally,
        // before ever redirecting to the provider: it does not look like a
        // signed token (three dot-separated segments).
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/admin_consent", get(admin_consent))
            .with_state(state);

        let raw_uuid = Uuid::new_v4().to_string();
        let req = HttpRequest::builder()
            .uri(format!("/v1/oauth/slack/admin_consent?state={raw_uuid}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Serializes the two tests below that mutate the process-global
    /// `OAUTH_STATE_SECRET` env var — `cargo test` runs `#[tokio::test]`s
    /// concurrently within one process, so an unguarded `set_var`/
    /// `remove_var` here could race a concurrent test's read of the same
    /// var. Same pattern as `workers::huddle`'s / `routes::slack_events`'s
    /// identically-shaped `*_ENV_LOCK` statics; local to this module since
    /// `route_migration_org_pg.rs`'s own `ENV_LOCK` lives in a separate test
    /// target (a distinct process) and cannot coordinate with this one
    /// anyway.
    static OAUTH_STATE_SECRET_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn admin_consent_token_shaped_but_unsigned_state_returns_400() {
        // Passes the cheap structural check (three dot-separated segments)
        // but carries no valid HMAC signature — must still be rejected,
        // proving `admin_consent` performs the SAME cryptographic
        // verification `callback` does, not merely the shape check.
        let _guard = OAUTH_STATE_SECRET_ENV_LOCK.lock().await;
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        unsafe {
            std::env::set_var("OAUTH_STATE_SECRET", "admin-consent-test-secret-0123456789");
        }
        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/admin_consent", get(admin_consent))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri("/v1/oauth/slack/admin_consent?state=aaa.bbb.ccc")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        drop(_guard);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn admin_consent_valid_signed_state_passes_verification() {
        // A genuinely signed, unexpired state token must pass this handler's
        // verification step. `slack` has no `admin_consent_url` override
        // (trait default: `None`), so a passing verification surfaces as the
        // DISTINCT "does not support org-wide admin consent" 400 — proving
        // this is a later failure than state validation, not the same one
        // the tests above assert.
        let _guard = OAUTH_STATE_SECRET_ENV_LOCK.lock().await;
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "admin-consent-test-secret-abcdefghijkl";
        unsafe {
            std::env::set_var("OAUTH_STATE_SECRET", secret);
        }
        let token = oauth_state::sign_state(
            secret,
            Uuid::new_v4(),
            Utc::now().timestamp(),
            "admin-consent-test-nonce",
            None,
            None,
        )
        .unwrap();

        let state = test_state();
        let router = Router::new()
            .route("/v1/oauth/{provider}/admin_consent", get(admin_consent))
            .with_state(state);

        let req = HttpRequest::builder()
            .uri(format!("/v1/oauth/slack/admin_consent?state={token}"))
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
        drop(_guard);

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(
            json["error"].as_str().unwrap().contains("does not support"),
            "a validly-signed state must reach the provider-capability check, \
             not fail state validation: {json:?}"
        );
    }

    // `derive_account_dek`'s formula tests RELOCATED (Foundation PR 1.2 /
    // CS-031): the function itself moved to `crate::crypto::
    // derive_legacy_oauth_account_dek` (retired from this file's request
    // path, kept only for the wrapped-DEK cutover backfill) — see
    // `crypto::tests::derive_legacy_dek_formula_is_deterministic` and
    // `derive_legacy_dek_different_account_ids_produce_different_deks`.

    // ── list_connections returns no token material ────────────────────────────
    //
    // The response type `OAuthConnection` is defined without access_token or
    // refresh_token fields.  This is a compile-time guarantee backed by a
    // serialisation test.

    #[test]
    fn oauth_connection_response_excludes_token_fields() {
        let conn = OAuthConnection {
            provider: "slack".into(),
            provider_workspace_id: Some("T123".into()),
            scope: "channels:read".into(),
            expires_at: None,
            connected_at: Utc::now(),
            missing_scopes: Vec::new(),
            scopes_unknown: false,
            credential_invalid_at: None,
            credential_invalid_reason: None,
            mine: false,
        };

        let json_str = serde_json::to_string(&conn).unwrap();
        let json_val: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Verify the response object has expected safe keys.
        assert!(json_val.get("provider").is_some());
        assert!(json_val.get("scope").is_some());
        assert!(json_val.get("connected_at").is_some());

        // Verify no token material is present.
        assert!(
            json_val.get("access_token").is_none(),
            "access_token must not appear in connections list"
        );
        assert!(
            json_val.get("refresh_token").is_none(),
            "refresh_token must not appear in connections list"
        );
    }

    // ── scope_reconsent_status (CS-431) ────────────────────────────────────

    #[test]
    fn scope_reconsent_status_slack_lists_missing_users_read_email() {
        let stored = "channels:history channels:read users:read reactions:read \
                       files:read canvases:read chat:write im:write channels:leave";
        let (missing, unknown) = scope_reconsent_status("slack", stored);
        assert_eq!(missing, vec!["users:read.email".to_string()]);
        assert!(!unknown);
    }

    #[test]
    fn scope_reconsent_status_slack_empty_when_fully_granted() {
        let (missing, unknown) =
            scope_reconsent_status("slack", &connectors::slack::slack_scopes());
        assert!(missing.is_empty(), "{missing:?}");
        assert!(!unknown);
    }

    #[test]
    fn scope_reconsent_status_slack_unknown_for_the_backfill_placeholder() {
        let (missing, unknown) = scope_reconsent_status(
            "slack",
            crate::slack_oauth_backfill::BACKFILLED_SCOPE_PLACEHOLDER,
        );
        assert!(
            missing.is_empty(),
            "never guess a missing list alongside unknown: {missing:?}"
        );
        assert!(unknown);
    }

    #[test]
    fn scope_reconsent_status_google_chat_lists_missing_directory_readonly() {
        let stored = "openid email https://www.googleapis.com/auth/chat.spaces.readonly \
                       https://www.googleapis.com/auth/chat.messages.readonly \
                       https://www.googleapis.com/auth/chat.memberships.readonly";
        let (missing, unknown) = scope_reconsent_status("google_chat", stored);
        assert_eq!(
            missing,
            vec!["https://www.googleapis.com/auth/directory.readonly".to_string()]
        );
        assert!(!unknown);
    }

    #[test]
    fn scope_reconsent_status_google_chat_unknown_for_a_blank_grant() {
        let (missing, unknown) = scope_reconsent_status("google_chat", "");
        assert!(missing.is_empty(), "{missing:?}");
        assert!(unknown);
    }

    /// Every other provider — Calendar (`"google"`), `"microsoft"`,
    /// `"google_gmail"` — has no known stale-grant case yet, so this must
    /// never compute a diff for them: `([], false)` unconditionally.
    #[test]
    fn scope_reconsent_status_other_providers_are_always_empty_and_known() {
        for provider in ["google", "microsoft", "google_gmail", ""] {
            let (missing, unknown) = scope_reconsent_status(provider, "");
            assert!(missing.is_empty(), "{provider}: {missing:?}");
            assert!(!unknown, "{provider}");
        }
    }

    // ── Org governance decision (2026-08-04), piece 1: admin-only ─────────────
    // connect/disconnect. `reject_org_slack_non_admin` is a pure function
    // (no DB access, same reason `is_admin_role` itself is DB-free) — it is
    // the SAME gate called by `mint_state` (connect), `routes::slack::
    // oauth_start` (connect), `delete_connection`'s Slack arm (disconnect),
    // and `routes::slack::delete_integration` (disconnect), so proving its
    // behavior here proves all four call sites at once. A lazy pool never
    // connects unless a query executes against it (no query ever does, on
    // this path) — same pattern `tenancy::reachability`'s own `lazy_ctx` test
    // helper uses, cited in its own doc comment.

    fn org_tenant_for_test() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_governance_test".to_string(),
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

    fn personal_tenant_for_test() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    // `#[tokio::test]`, not `#[test]`: `PgPoolOptions::connect_lazy` itself
    // needs a Tokio context to construct (even though it never connects),
    // same requirement `tenancy::reachability`'s `lazy_ctx` helper's own
    // callers satisfy by being async tests.

    #[tokio::test]
    async fn reject_org_slack_non_admin_blocks_plain_member() {
        let tenant = org_tenant_for_test();
        let resp = reject_org_slack_non_admin(&tenant, "member");
        assert!(resp.is_some(), "a plain org member must be refused");
        assert_eq!(resp.unwrap().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reject_org_slack_non_admin_allows_admin() {
        let tenant = org_tenant_for_test();
        assert!(
            reject_org_slack_non_admin(&tenant, "admin").is_none(),
            "an org admin must be allowed through"
        );
    }

    #[tokio::test]
    async fn reject_org_slack_non_admin_allows_owner() {
        let tenant = org_tenant_for_test();
        assert!(
            reject_org_slack_non_admin(&tenant, "owner").is_none(),
            "an org owner must be allowed through"
        );
    }

    #[test]
    fn reject_org_slack_non_admin_is_noop_on_the_personal_plane() {
        let tenant = personal_tenant_for_test();
        // Personal accounts are UNCHANGED by this decision — there is no org
        // context to be a non-admin OF, so even a `"member"`-shaped role
        // (which a personal account's `UserContext::role` never actually is —
        // it is always `"owner"`, see `middleware::auth::resolve_clerk_context`'s
        // `None` arm — but this proves the gate does not accidentally start
        // caring about role once org_context is absent) is never refused.
        assert!(reject_org_slack_non_admin(&tenant, "member").is_none());
    }

    // ── gsuite Train G2 PR 6: Gmail org-plane gate lift (org_google_gmail_
    // enabled_for / reject_org_gmail_management / google_org_callback_allowed) ──
    //
    // No DB needed: `state.db` is `None` on `test_state()`, so
    // `connector_flags::resolve` falls back to the raw env-var default
    // (`connector_flags::resolve`'s own doc comment) — exactly the same
    // no-control-pool fallback path the EXISTING `authorize_google_gmail_*`
    // tests above already exercise for the personal-plane surface gate.
    // `GOOGLE_SURFACE_ENV_LOCK` (declared with those tests) serializes every
    // test in this file that mutates `ORG_GOOGLE_GMAIL_ENABLED`.

    #[tokio::test]
    async fn reject_org_gmail_management_blocks_org_when_flag_off() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") };
        let tenant = org_tenant_for_test();
        let resp = reject_org_gmail_management(&test_state(), &tenant).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        assert!(resp.is_some(), "flag off must refuse the org caller");
        assert_eq!(resp.unwrap().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reject_org_gmail_management_allows_org_when_flag_on() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true") };
        let tenant = org_tenant_for_test();
        let resp = reject_org_gmail_management(&test_state(), &tenant).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        assert!(resp.is_none(), "flag on must let the org caller through");
    }

    #[tokio::test]
    async fn reject_org_gmail_management_is_noop_on_the_personal_plane() {
        // Flag state irrelevant — `org_context()` is `None` for Personal, so
        // this returns `None` (allow) unconditionally, byte-identical to
        // every existing personal-plane connector route.
        let tenant = personal_tenant_for_test();
        assert!(reject_org_gmail_management(&test_state(), &tenant)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_gmail_flag_on_is_allowed() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true") };
        let allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("gmail")).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        assert!(allowed, "org + flag on + gmail must complete");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_gmail_flag_off_is_refused() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") };
        let allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("gmail")).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        assert!(!allowed, "org + flag off + gmail must stay refused");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_gmail_flag_on_calendar_surface_still_refused() {
        // `state_surface` absent/unparseable defaults to Calendar
        // (`resolve_google_token_target`'s own convention) — proves the
        // Gmail flag does NOT accidentally widen to Calendar just because
        // `provider_id == "google"` matches.
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true") };
        let allowed_no_surface =
            google_org_callback_allowed(&test_state(), "org_test", "google", None).await;
        let allowed_calendar_surface =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("calendar"))
                .await;
        let allowed_chat_surface =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("chat")).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        assert!(
            !allowed_no_surface,
            "no surface must default-refuse, not default-allow"
        );
        assert!(
            !allowed_calendar_surface,
            "Calendar must stay refused under org regardless of the Gmail flag"
        );
        assert!(
            !allowed_chat_surface,
            "Chat must stay refused under org regardless of the Gmail flag"
        );
    }

    #[tokio::test]
    async fn google_org_callback_allowed_slack_and_gmail_flags_are_independent() {
        // Slack flag ON, Gmail flag OFF: a Gmail callback must still be
        // refused — the two providers' gates are never OR'd together.
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior_slack = std::env::var("ORG_SLACK_OAUTH_ENABLED").ok();
        let prior_gmail = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        unsafe {
            std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
            std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED");
        }
        let gmail_allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("gmail")).await;
        let slack_allowed =
            google_org_callback_allowed(&test_state(), "org_test", "slack", None).await;
        match prior_slack {
            Some(v) => unsafe { std::env::set_var("ORG_SLACK_OAUTH_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_SLACK_OAUTH_ENABLED") },
        }
        match prior_gmail {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        assert!(
            slack_allowed,
            "Slack's own flag being on must still allow Slack"
        );
        assert!(
            !gmail_allowed,
            "Slack's flag must never leak into Gmail's own gate decision"
        );
    }

    // ── gsuite Train G4 PR 6 ("G4.6"): Chat org-plane gate lift
    // (org_google_chat_enabled_for / reject_org_chat_management /
    // google_org_callback_allowed's new Chat arm) — mirrors the Gmail block
    // immediately above, one surface later. ─────────────────────────────────

    #[tokio::test]
    async fn reject_org_chat_management_blocks_org_when_flag_off() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") };
        let tenant = org_tenant_for_test();
        let resp = reject_org_chat_management(&test_state(), &tenant).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        assert!(resp.is_some(), "flag off must refuse the org caller");
        assert_eq!(resp.unwrap().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reject_org_chat_management_allows_org_when_flag_on() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true") };
        let tenant = org_tenant_for_test();
        let resp = reject_org_chat_management(&test_state(), &tenant).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        assert!(resp.is_none(), "flag on must let the org caller through");
    }

    #[tokio::test]
    async fn reject_org_chat_management_is_noop_on_the_personal_plane() {
        let tenant = personal_tenant_for_test();
        assert!(reject_org_chat_management(&test_state(), &tenant)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_chat_flag_on_is_allowed() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true") };
        let allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("chat")).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        assert!(allowed, "org + flag on + chat must complete");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_chat_flag_off_is_refused() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") };
        let allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("chat")).await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        assert!(!allowed, "org + flag off + chat must stay refused");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_gmail_and_chat_flags_are_independent() {
        // Gmail flag ON, Chat flag OFF: a Chat callback must still be
        // refused, and vice versa — the two surfaces' gates are never OR'd
        // together, same non-OR rule `google_org_callback_allowed`'s own doc
        // comment states for every provider/surface pair.
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior_gmail = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        let prior_chat = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        unsafe {
            std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
            std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED");
        }
        let gmail_allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("gmail")).await;
        let chat_allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("chat")).await;
        match prior_gmail {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        match prior_chat {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        assert!(
            gmail_allowed,
            "Gmail's own flag being on must still allow Gmail"
        );
        assert!(
            !chat_allowed,
            "Gmail's flag must never leak into Chat's own gate decision"
        );
    }

    // ── Meet org-plane lift (org_google_meet_enabled_for /
    // reject_org_meet_management / google_org_callback_allowed's Meet arm) —
    // mirrors the Gmail and Chat blocks above, one surface later. ───────────

    /// Run `body` with `ORG_GOOGLE_MEET_ENABLED` forced to `value` (`None` =
    /// unset), restoring the prior value afterwards. Holds
    /// `GOOGLE_SURFACE_ENV_LOCK` for the duration.
    async fn with_meet_flag<F, Fut, T>(value: Option<&str>, body: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_MEET_ENABLED").ok();
        match value {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_MEET_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_MEET_ENABLED") },
        }
        let out = body().await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_MEET_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_MEET_ENABLED") },
        }
        out
    }

    #[tokio::test]
    async fn org_google_meet_enabled_for_defaults_off() {
        // The flag default: with no registry row and no env var, Meet is OFF
        // for every org — same default as Gmail's and Chat's.
        let on = with_meet_flag(None, || async {
            org_google_meet_enabled_for(&test_state(), "org_test").await
        })
        .await;
        assert!(!on, "Meet must default OFF for an org with no override and no env");
    }

    #[tokio::test]
    async fn org_google_meet_enabled_for_follows_the_env_default() {
        let on = with_meet_flag(Some("true"), || async {
            org_google_meet_enabled_for(&test_state(), "org_test").await
        })
        .await;
        assert!(on);
        let off = with_meet_flag(Some("nope"), || async {
            org_google_meet_enabled_for(&test_state(), "org_test").await
        })
        .await;
        assert!(!off, "a typo fails closed");
    }

    #[tokio::test]
    async fn reject_org_meet_management_blocks_org_when_flag_off() {
        let resp = with_meet_flag(None, || async {
            reject_org_meet_management(&test_state(), &org_tenant_for_test()).await
        })
        .await;
        assert_eq!(resp.expect("flag off must refuse").status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reject_org_meet_management_allows_org_when_flag_on() {
        let resp = with_meet_flag(Some("true"), || async {
            reject_org_meet_management(&test_state(), &org_tenant_for_test()).await
        })
        .await;
        assert!(resp.is_none(), "flag on must let the org caller through");
    }

    #[tokio::test]
    async fn reject_org_meet_management_is_noop_on_the_personal_plane() {
        let resp = with_meet_flag(None, || async {
            reject_org_meet_management(&test_state(), &personal_tenant_for_test()).await
        })
        .await;
        assert!(resp.is_none(), "personal plane is never gated by the org flag");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_meet_flag_on_is_allowed() {
        let allowed = with_meet_flag(Some("true"), || async {
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("meet")).await
        })
        .await;
        assert!(allowed, "org + flag on + meet must complete");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_meet_flag_off_is_refused() {
        let allowed = with_meet_flag(None, || async {
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("meet")).await
        })
        .await;
        assert!(!allowed, "org + flag off + meet must stay refused");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_meet_flag_never_widens_to_calendar_or_other_surfaces() {
        let (cal, none, gmail, chat) = with_meet_flag(Some("true"), || async {
            let s = test_state();
            (
                google_org_callback_allowed(&s, "org_test", "google", Some("calendar")).await,
                google_org_callback_allowed(&s, "org_test", "google", None).await,
                google_org_callback_allowed(&s, "org_test", "google", Some("gmail")).await,
                google_org_callback_allowed(&s, "org_test", "google", Some("chat")).await,
            )
        })
        .await;
        assert!(!cal, "Calendar stays refused under org regardless of the Meet flag");
        assert!(!none, "no surface must default-refuse");
        // Gmail/Chat flags are unset in this test (their own env defaults are
        // off), so the Meet flag must not leak into them.
        assert!(!gmail && !chat, "the Meet flag must never leak into Gmail's or Chat's decision");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_gmail_and_chat_flags_never_admit_meet() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior_meet = std::env::var("ORG_GOOGLE_MEET_ENABLED").ok();
        let prior_gmail = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        let prior_chat = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        unsafe {
            std::env::remove_var("ORG_GOOGLE_MEET_ENABLED");
            std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
            std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true");
        }
        let meet_allowed =
            google_org_callback_allowed(&test_state(), "org_test", "google", Some("meet")).await;
        match prior_meet {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_MEET_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_MEET_ENABLED") },
        }
        match prior_gmail {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        match prior_chat {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        assert!(!meet_allowed, "Gmail/Chat flags on must never admit Meet — each surface gates on its own flag");
    }

    // ── Google Calendar org lift (org_google_calendar_enabled_for /
    // reject_org_calendar_management / google_org_callback_allowed's Calendar
    // arm / resolve_google_token_target's org arm) — mirrors the Meet block
    // above. ──────────────────────────────────────────────────────────────

    /// Run `body` with `ORG_GOOGLE_CALENDAR_ENABLED` forced to `value` (`None`
    /// = unset), restoring the prior value afterwards. Holds
    /// `GOOGLE_SURFACE_ENV_LOCK` for the duration.
    async fn with_calendar_flag<F, Fut, T>(value: Option<&str>, body: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_CALENDAR_ENABLED").ok();
        match value {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
        }
        let out = body().await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
        }
        out
    }

    #[tokio::test]
    async fn org_google_calendar_enabled_for_defaults_off() {
        let on = with_calendar_flag(None, || async {
            org_google_calendar_enabled_for(&test_state(), "org_test").await
        })
        .await;
        assert!(!on, "Calendar must default OFF for an org with no override and no env");
    }

    #[tokio::test]
    async fn org_google_calendar_enabled_for_follows_the_env_default_and_fails_closed() {
        let on = with_calendar_flag(Some("true"), || async {
            org_google_calendar_enabled_for(&test_state(), "org_test").await
        })
        .await;
        assert!(on);
        let one = with_calendar_flag(Some("1"), || async {
            org_google_calendar_enabled_for(&test_state(), "org_test").await
        })
        .await;
        assert!(one);
        let off = with_calendar_flag(Some("nope"), || async {
            org_google_calendar_enabled_for(&test_state(), "org_test").await
        })
        .await;
        assert!(!off, "a typo fails closed");
    }

    #[tokio::test]
    async fn reject_org_calendar_management_blocks_org_when_flag_off() {
        let resp = with_calendar_flag(None, || async {
            reject_org_calendar_management(&test_state(), &org_tenant_for_test()).await
        })
        .await;
        assert_eq!(resp.expect("flag off must refuse").status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn reject_org_calendar_management_allows_org_when_flag_on() {
        let resp = with_calendar_flag(Some("true"), || async {
            reject_org_calendar_management(&test_state(), &org_tenant_for_test()).await
        })
        .await;
        assert!(resp.is_none(), "flag on must let the org caller through");
    }

    #[tokio::test]
    async fn reject_org_calendar_management_is_noop_on_the_personal_plane() {
        let resp = with_calendar_flag(None, || async {
            reject_org_calendar_management(&test_state(), &personal_tenant_for_test()).await
        })
        .await;
        assert!(resp.is_none(), "the personal plane is never gated by the org flag");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_calendar_flag_on_is_allowed_for_explicit_and_absent_surface() {
        let (explicit, absent) = with_calendar_flag(Some("true"), || async {
            let s = test_state();
            (
                google_org_callback_allowed(&s, "org_test", "google", Some("calendar")).await,
                // A pre-surface-split `sfc`-less state resolves to Calendar in
                // `resolve_google_token_target`, so the gate must agree.
                google_org_callback_allowed(&s, "org_test", "google", None).await,
            )
        })
        .await;
        assert!(explicit && absent, "org + flag on + calendar (explicit or absent) must complete");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_org_calendar_flag_off_is_refused() {
        let (explicit, absent) = with_calendar_flag(None, || async {
            let s = test_state();
            (
                google_org_callback_allowed(&s, "org_test", "google", Some("calendar")).await,
                google_org_callback_allowed(&s, "org_test", "google", None).await,
            )
        })
        .await;
        assert!(!explicit && !absent, "org + flag off must keep refusing Calendar, however it is spelled");
    }

    #[tokio::test]
    async fn google_org_callback_allowed_calendar_flag_never_widens_to_other_surfaces_or_garbage() {
        let (gmail, chat, meet, garbage) = with_calendar_flag(Some("true"), || async {
            let s = test_state();
            (
                google_org_callback_allowed(&s, "org_test", "google", Some("gmail")).await,
                google_org_callback_allowed(&s, "org_test", "google", Some("chat")).await,
                google_org_callback_allowed(&s, "org_test", "google", Some("meet")).await,
                // An unparseable non-empty claim is NOT Calendar here.
                google_org_callback_allowed(&s, "org_test", "google", Some("drive")).await,
            )
        })
        .await;
        assert!(
            !gmail && !chat && !meet && !garbage,
            "the Calendar flag must admit Calendar only: gmail={gmail} chat={chat} meet={meet} garbage={garbage}"
        );
    }

    #[tokio::test]
    async fn google_org_callback_allowed_other_google_flags_never_admit_calendar() {
        let _guard = GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let priors: Vec<(&str, Option<String>)> = ["ORG_GOOGLE_GMAIL_ENABLED", "ORG_GOOGLE_CHAT_ENABLED", "ORG_GOOGLE_MEET_ENABLED", "ORG_GOOGLE_CALENDAR_ENABLED"]
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();
        for k in ["ORG_GOOGLE_GMAIL_ENABLED", "ORG_GOOGLE_CHAT_ENABLED", "ORG_GOOGLE_MEET_ENABLED"] {
            unsafe { std::env::set_var(k, "true") };
        }
        unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") };
        let s = test_state();
        let cal_explicit = google_org_callback_allowed(&s, "org_test", "google", Some("calendar")).await;
        let cal_absent = google_org_callback_allowed(&s, "org_test", "google", None).await;
        for (k, v) in priors {
            match v {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        assert!(
            !cal_explicit && !cal_absent,
            "Gmail/Chat/Meet flags on must never admit Calendar — each surface gates on its own flag"
        );
    }

    #[test]
    fn resolve_google_token_target_calendar_org_plane_is_per_member_under_the_google_literal() {
        let uid = Uuid::new_v4();
        // Explicit and absent surface both resolve to Calendar.
        for claim in [Some("calendar"), None] {
            let (owner, provider) = resolve_google_token_target(claim, Some(uid), true).unwrap();
            assert_eq!(owner, TokenOwner::Member(uid), "org Calendar is per-member");
            assert_eq!(provider, "google", "same grandfathered provider literal as personal");
        }
    }

    #[test]
    fn resolve_google_token_target_calendar_org_plane_without_uid_is_refused_not_a_fallback() {
        assert_eq!(resolve_google_token_target(Some("calendar"), None, true), Err(()));
        assert_eq!(resolve_google_token_target(None, None, true), Err(()));
    }

    #[test]
    fn resolve_google_token_target_calendar_personal_plane_is_unchanged_account_wide() {
        let uid = Uuid::new_v4();
        let (owner, provider) = resolve_google_token_target(Some("calendar"), Some(uid), false).unwrap();
        assert_eq!(owner, TokenOwner::AccountWide);
        assert_eq!(provider, "google");
        let (owner, _) = resolve_google_token_target(None, None, false).unwrap();
        assert_eq!(owner, TokenOwner::AccountWide);
    }

    #[test]
    fn resolve_delete_token_owner_google_follows_the_plane_like_the_write_path() {
        let uid = Uuid::new_v4();
        assert_eq!(resolve_delete_token_owner("google", false, uid), TokenOwner::AccountWide);
        assert_eq!(resolve_delete_token_owner("google", true, uid), TokenOwner::Member(uid));
    }

    #[tokio::test]
    async fn google_org_callback_allowed_microsoft_gated_on_its_own_flag() {
        // Post-m365 PR 3.3: Microsoft IS admitted under an org — but only
        // while the org's `"microsoft"` registry flag resolves on. With no
        // row and no `ORG_MICROSOFT_TEAMS_ENABLED` env (this test's state),
        // the flag resolves off → refused. The flag-ON positive branch is
        // exercised by `reject_org_connector_access`'s own pg tests against
        // a real registry row (same flag, same resolver) rather than racing
        // a process-global env var here.
        let allowed =
            google_org_callback_allowed(&test_state(), "org_test", "microsoft", None).await;
        assert!(
            !allowed,
            "microsoft with its org flag off must still be refused"
        );
    }

    /// Pins every provider-blind org flag that `mint_state`'s fall-through arm
    /// (`reject_org_oauth_management`) ORs together to "off" for the lifetime
    /// of the guard, restoring the prior values on drop (also on panic).
    ///
    /// The `mint_state_org_{gmail,chat}_surface_gated_...` tests end by
    /// asserting that a surface-less org mint is refused, i.e. that the
    /// Gmail/Chat flag does not leak into that arm. That refusal holds only
    /// while Slack, Microsoft and GitHub are all off, and
    /// `slack_member_connect_pg_tests` sets `ORG_SLACK_OAUTH_ENABLED=true`
    /// (under its own `ENV_LOCK`, not the Google locks) and does not always
    /// restore it, so in a full-module run the mint returned 200. Callers
    /// therefore hold that module's `ENV_LOCK` as well as the Google locks.
    struct ProviderFlagsPinnedOff {
        prior: Vec<(&'static str, Option<String>)>,
    }

    impl ProviderFlagsPinnedOff {
        fn new() -> Self {
            const VARS: [&str; 3] = [
                "ORG_SLACK_OAUTH_ENABLED",
                "ORG_MICROSOFT_TEAMS_ENABLED",
                "ORG_GITHUB_ENABLED",
            ];
            let prior = VARS.iter().map(|v| (*v, std::env::var(v).ok())).collect();
            // SAFETY: every caller holds the Google env locks and
            // `slack_member_connect_pg_tests::ENV_LOCK`, which together
            // serialize the tests in this crate that write these variables.
            for v in VARS {
                unsafe { std::env::remove_var(v) };
            }
            Self { prior }
        }
    }

    impl Drop for ProviderFlagsPinnedOff {
        fn drop(&mut self) {
            for (name, value) in &self.prior {
                // SAFETY: as in `new`; the locks outlive this guard.
                unsafe {
                    match value {
                        Some(v) => std::env::set_var(name, v),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn mint_state_org_gmail_surface_gated_on_its_own_flag_not_slack_admin() {
        // Calls `mint_state` directly (State/Tenant/Extension/Query
        // constructed by hand — same "real production handler, no router"
        // convention `list_connections`/`delete_connection`'s own pg tests
        // use), as a PLAIN org member (`role: "member"`, never admin) —
        // proves Gmail's mint_state branch does NOT run
        // `reject_org_slack_non_admin` (a plain member is refused by THAT
        // gate for a Slack-shaped request, but must be ALLOWED for Gmail).
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let _g3 = super::slack_member_connect_pg_tests::ENV_LOCK.lock().await;
        let _provider_flags_off = ProviderFlagsPinnedOff::new();
        let prior_flag = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "mint-state-gmail-org-test-secret-0123456789";
        unsafe {
            std::env::set_var("OAUTH_STATE_SECRET", secret);
            std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED");
        }
        let member_ctx = UserContext {
            user_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };

        // ── flag OFF: refused, even though this is a plain (non-admin)
        // member — proves the refusal is the FLAG, not an admin check. ─────
        let resp = mint_state(
            State(test_state()),
            org_tenant_for_test(),
            Extension(member_ctx.clone()),
            Query(MintStateParams {
                surface: Some("gmail".to_string()),
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "org + flag off must refuse mint_state for gmail"
        );

        // ── flag ON: a PLAIN MEMBER succeeds (no admin check for Gmail) ────
        unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true") };
        let resp = mint_state(
            State(test_state()),
            org_tenant_for_test(),
            Extension(member_ctx.clone()),
            Query(MintStateParams {
                surface: Some("gmail".to_string()),
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "org + flag on must mint a state for a plain member requesting gmail"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_val: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let token = json_val["state"].as_str().expect("state field present");
        let verified = oauth_state::verify_state(secret, token).expect("state verifies");
        assert_eq!(verified.user_id, Some(member_ctx.user_id));
        assert_eq!(verified.surface.as_deref(), Some("gmail"));

        // ── Same plain member, requesting a PLAIN (Slack-shaped) state with
        // no `?surface=` — still refused: the Gmail flag does not leak into
        // the Slack-only branch, and this member is not an admin. ─────────
        let resp = mint_state(
            State(test_state()),
            org_tenant_for_test(),
            Extension(member_ctx.clone()),
            Query(MintStateParams { surface: None }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "the Gmail flag must not leak into the Slack-only mint_state branch"
        );

        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
    }

    #[tokio::test]
    async fn mint_state_org_chat_surface_gated_on_its_own_flag_not_slack_admin() {
        // Mirrors `mint_state_org_gmail_surface_gated_on_its_own_flag_not_
        // slack_admin` exactly, one surface later (gsuite Train G4 PR 6):
        // Chat's `mint_state` branch does NOT run `reject_org_slack_non_
        // admin` either — a plain org member must be ALLOWED to mint a Chat
        // state once the flag is on.
        let (_g1, _g2) = lock_google_surface_and_state_secret_env().await;
        let _g3 = super::slack_member_connect_pg_tests::ENV_LOCK.lock().await;
        let _provider_flags_off = ProviderFlagsPinnedOff::new();
        let prior_flag = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
        let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
        let secret = "mint-state-chat-org-test-secret-0123456789";
        unsafe {
            std::env::set_var("OAUTH_STATE_SECRET", secret);
            std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED");
        }
        let member_ctx = UserContext {
            user_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };

        // ── flag OFF: refused, even though this is a plain (non-admin)
        // member — proves the refusal is the FLAG, not an admin check. ─────
        let resp = mint_state(
            State(test_state()),
            org_tenant_for_test(),
            Extension(member_ctx.clone()),
            Query(MintStateParams {
                surface: Some("chat".to_string()),
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "org + flag off must refuse mint_state for chat"
        );

        // ── flag ON: a PLAIN MEMBER succeeds (no admin check for Chat) ─────
        unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true") };
        let resp = mint_state(
            State(test_state()),
            org_tenant_for_test(),
            Extension(member_ctx.clone()),
            Query(MintStateParams {
                surface: Some("chat".to_string()),
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "org + flag on must mint a state for a plain member requesting chat"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_val: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let token = json_val["state"].as_str().expect("state field present");
        let verified = oauth_state::verify_state(secret, token).expect("state verifies");
        assert_eq!(verified.user_id, Some(member_ctx.user_id));
        assert_eq!(verified.surface.as_deref(), Some("chat"));

        // ── Same plain member, requesting a PLAIN (Slack-shaped) state with
        // no `?surface=` — still refused: the Chat flag does not leak into
        // the Slack-only branch, and this member is not an admin. ─────────
        let resp = mint_state(
            State(test_state()),
            org_tenant_for_test(),
            Extension(member_ctx.clone()),
            Query(MintStateParams { surface: None }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "the Chat flag must not leak into the Slack-only mint_state branch"
        );

        match prior_flag {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
        }
        match prior_secret {
            Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
            None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
        }
    }

    // ── PR G2.1: per-surface + per-member Google token isolation (pg-gated) ──
    //
    // Against a REAL Postgres database (unlike everything above in this
    // module): proves the schema-level invariant the whole PR rests on — a
    // Calendar `AccountWide` row and per-member Gmail `Member` rows coexist
    // under the SAME `(account_id, provider)` pair (item 1's distinct
    // provider literals) without colliding, that a per-owner lookup can never
    // cross into another owner's row in either direction (contract I7), and
    // that `list_connections`/`delete_connection` (the REAL, unmodified
    // production functions — called directly, not through a router) are
    // owner-scoped exactly as item 5 specifies.
    //
    //   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
    //     cargo test -p coralswarm-backend --lib routes::oauth::tests::google_token_isolation_pg \
    //     -- --test-threads=1
    mod google_token_isolation_pg {
        use super::*;
        use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
        use sqlx::postgres::PgPoolOptions;
        use sqlx::{Connection, Executor, PgConnection};

        fn base_url() -> Option<String> {
            std::env::var("ORGT_TEST_BASE_URL").ok()
        }
        fn with_db(base: &str, db: &str) -> String {
            let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
            let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
            format!("{prefix}/{db}{params}")
        }
        async fn recreate(base: &str, db: &str) -> PgConnection {
            let mut conn = PgConnection::connect(base).await.expect("connect admin");
            conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .unwrap();
            conn.execute(format!("CREATE DATABASE {db}").as_str())
                .await
                .unwrap();
            conn
        }
        async fn teardown(base: &str, db: &str, pool: PgPool) {
            pool.close().await;
            let mut admin_conn = PgConnection::connect(base).await.unwrap();
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
        /// Same fixed-KEK convention as `worker_dek_egress_pg.rs` /
        /// `dek_relocation_pg.rs`: every test in this process sets
        /// `MASTER_KEK_HEX` to the SAME constant, so a concurrent write from
        /// another test is benign (identical bytes).
        fn set_test_kek() {
            // SAFETY: see this fn's own doc comment above.
            unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
        }
        async fn seed_user(pool: &PgPool) -> Uuid {
            sqlx::query_scalar(
                "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
            )
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
        }
        /// An account with a real wrapped DEK already provisioned — every
        /// `store_token_bundle_tx` call needs one (`crypto::resolve_dek`).
        async fn seed_account_with_dek(pool: &PgPool, owner: Uuid) -> Uuid {
            let kek = load_kek().expect("test KEK loads");
            let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
            sqlx::query_scalar(
                "INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner)
            .bind(&wrapped)
            .fetch_one(pool)
            .await
            .unwrap()
        }
        fn calendar_bundle(email: &str) -> TokenBundle {
            TokenBundle {
                access_token: format!("calendar-access-{email}"),
                refresh_token: Some(format!("calendar-refresh-{email}")),
                user_token: None,
                scope: "openid email https://www.googleapis.com/auth/calendar.readonly".into(),
                expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
                provider_workspace_id: email.to_string(),
                provider_workspace_name: Some(email.to_string()),
                provider_user_id: format!("sub-{email}"),
            }
        }
        fn gmail_bundle(email: &str) -> TokenBundle {
            TokenBundle {
                access_token: format!("gmail-access-{email}"),
                refresh_token: Some(format!("gmail-refresh-{email}")),
                user_token: None,
                scope: "openid email https://www.googleapis.com/auth/gmail.readonly".into(),
                expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
                provider_workspace_id: email.to_string(),
                provider_workspace_name: Some(email.to_string()),
                provider_user_id: format!("sub-{email}"),
            }
        }
        /// gsuite Train G4 PR 1 ("G4.1"): the amended, three-scope Chat
        /// bundle — carries `connectors::google::Surface::Chat::data_scope()`
        /// verbatim (including the newly added `chat.memberships.readonly`)
        /// so this module's pg tests exercise the SAME scope string
        /// `callback` would actually persist post-amendment, not a
        /// hand-rolled stand-in that could quietly drift from it.
        fn chat_bundle(sub_label: &str) -> TokenBundle {
            TokenBundle {
                access_token: format!("chat-access-{sub_label}"),
                refresh_token: Some(format!("chat-refresh-{sub_label}")),
                user_token: None,
                scope: format!(
                    "openid email {}",
                    connectors::google::Surface::Chat.data_scope()
                ),
                expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
                provider_workspace_id: format!("workspace-{sub_label}"),
                provider_workspace_name: Some(format!("workspace-{sub_label}")),
                provider_user_id: format!("sub-{sub_label}"),
            }
        }
        /// A Slack account-wide bundle carrying [`connectors::slack::
        /// slack_scopes`] verbatim — CS-431's own re-consent-overwrite test
        /// (below) needs a bundle shaped exactly like a REAL callback would
        /// build one, same reasoning as `chat_bundle`'s own doc comment.
        /// `scope_override`, when given, replaces the granted scope string —
        /// how the test simulates "installed before `users:read.email`
        /// shipped" without hand-drifting from `slack_scopes()`'s real
        /// default.
        fn slack_bundle(team_id: &str, scope_override: Option<&str>) -> TokenBundle {
            TokenBundle {
                access_token: format!("slack-access-{team_id}"),
                refresh_token: None,
                user_token: None,
                scope: scope_override
                    .map(str::to_string)
                    .unwrap_or_else(connectors::slack::slack_scopes),
                expires_at: None,
                provider_workspace_id: team_id.to_string(),
                provider_workspace_name: Some(format!("Team {team_id}")),
                provider_user_id: format!("U-{team_id}"),
            }
        }
        /// CS-452: a Microsoft bundle, one code exchange covering mail/
        /// calendar/Teams together (`resolve_microsoft_token_target`'s own
        /// doc comment — there is no per-surface split the way Google has).
        /// `label` doubles as both `provider_workspace_id` (real Microsoft
        /// connections key this on the AAD tenant id) and a distinguishing
        /// suffix on the plaintext token, so CS-452's own owner-scoping pg
        /// tests can seed an `AccountWide` row and multiple `Member` rows
        /// for the SAME provider without a unique-index collision.
        fn microsoft_bundle(label: &str) -> TokenBundle {
            TokenBundle {
                access_token: format!("ms-access-{label}"),
                refresh_token: Some(format!("ms-refresh-{label}")),
                user_token: None,
                scope: "openid email offline_access Mail.Read Mail.ReadWrite \
                        Calendars.Read ChannelMessage.Read.All Chat.Read"
                    .to_string(),
                expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
                provider_workspace_id: format!("tenant-{label}"),
                provider_workspace_name: Some(format!("Tenant {label}")),
                provider_user_id: format!("aad-{label}"),
            }
        }
        /// The exact write [`callback`] performs (open a scoped tx, call the
        /// real `store_token_bundle_tx`, commit) — not a hand-rolled INSERT,
        /// so these tests prove the production upsert shape, including which
        /// partial unique index each owner variant hits.
        async fn store(
            pool: &PgPool,
            account_id: Uuid,
            provider: &str,
            owner: TokenOwner,
            bundle: &TokenBundle,
        ) {
            let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
                .await
                .expect("begin scoped tx");
            store_token_bundle_tx(
                &mut tx,
                &crate::crypto::DekPlane::consumer(pool.clone()),
                account_id,
                provider,
                owner,
                bundle,
            )
            .await
            .expect("store_token_bundle_tx");
            tx.commit().await.expect("commit");
        }

        /// Like [`store`], but ALSO writes the `connector_installs` mirror in
        /// the same transaction — the exact shape [`callback`] uses (CodeRabbit
        /// round-2 fix's own regression test needs both tables populated, not
        /// just `oauth_tokens`, to exercise the `connector_installs` DELETE's
        /// owner scoping).
        async fn store_with_install(
            pool: &PgPool,
            account_id: Uuid,
            provider: &str,
            owner: TokenOwner,
            bundle: &TokenBundle,
        ) {
            let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
                .await
                .expect("begin scoped tx");
            store_token_bundle_tx(
                &mut tx,
                &crate::crypto::DekPlane::consumer(pool.clone()),
                account_id,
                provider,
                owner,
                bundle,
            )
            .await
            .expect("store_token_bundle_tx");
            record_connector_install(
                &mut tx,
                account_id,
                provider,
                &bundle.provider_workspace_id,
                Some(bundle.provider_user_id.as_str()),
                owner.owner_user_id(),
            )
            .await
            .expect("record_connector_install");
            tx.commit().await.expect("commit");
        }

        #[tokio::test]
        async fn calendar_and_two_members_gmail_tokens_coexist_as_three_rows_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping calendar_and_two_members_gmail_tokens_coexist_as_three_rows_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "gws_g21_three_row_coexist";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member1 = seed_user(&pool).await;
            let member2 = seed_user(&pool).await;

            store(
                &pool,
                account_id,
                "google",
                TokenOwner::AccountWide,
                &calendar_bundle("acct@example.com"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member1),
                &gmail_bundle("member1@example.com"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member2),
                &gmail_bundle("member2@example.com"),
            )
            .await;

            let rows: Vec<(String, Option<Uuid>, String)> = sqlx::query_as(
                "SELECT provider, owner_user_id, provider_workspace_id FROM oauth_tokens \
                 WHERE account_id = $1 ORDER BY provider, provider_workspace_id",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap();

            assert_eq!(
                rows.len(),
                3,
                "calendar row + two members' gmail rows must coexist: {rows:?}"
            );
            assert_eq!(
                rows[0],
                ("google".to_string(), None, "acct@example.com".to_string()),
                "Calendar stays the grandfathered literal, account-wide"
            );
            assert_eq!(
                rows[1],
                (
                    "google_gmail".to_string(),
                    Some(member1),
                    "member1@example.com".to_string()
                )
            );
            assert_eq!(
                rows[2],
                (
                    "google_gmail".to_string(),
                    Some(member2),
                    "member2@example.com".to_string()
                )
            );

            teardown(&base, db, pool).await;
        }

        #[tokio::test]
        async fn i7_owner_lookup_never_falls_back_across_the_boundary_either_direction_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping i7_owner_lookup_never_falls_back_across_the_boundary_either_direction_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "gws_g21_i7_no_fallback";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member1 = seed_user(&pool).await;
            let member2 = seed_user(&pool).await;

            store(
                &pool,
                account_id,
                "google",
                TokenOwner::AccountWide,
                &calendar_bundle("acct@example.com"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member1),
                &gmail_bundle("member1@example.com"),
            )
            .await;

            // Mirrors `routes::connectors::load_creds`'s exact two WHERE-clause
            // shapes (`owner_user_id IS NULL` vs `owner_user_id = $n`) — the
            // production query `TokenOwner` exists to make impossible to blur
            // into a single `IS NULL OR = $n` fallback.
            async fn find_account_wide(
                pool: &PgPool,
                account_id: Uuid,
                provider: &str,
            ) -> Option<String> {
                sqlx::query_scalar(
                    "SELECT provider_workspace_id FROM oauth_tokens \
                     WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NULL",
                )
                .bind(account_id)
                .bind(provider)
                .fetch_optional(pool)
                .await
                .unwrap()
            }
            async fn find_member(
                pool: &PgPool,
                account_id: Uuid,
                provider: &str,
                owner_user_id: Uuid,
            ) -> Option<String> {
                sqlx::query_scalar(
                    "SELECT provider_workspace_id FROM oauth_tokens \
                     WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3",
                )
                .bind(account_id)
                .bind(provider)
                .bind(owner_user_id)
                .fetch_optional(pool)
                .await
                .unwrap()
            }

            // The two REAL lookups each find exactly their own row.
            assert_eq!(
                find_account_wide(&pool, account_id, "google")
                    .await
                    .as_deref(),
                Some("acct@example.com")
            );
            assert_eq!(
                find_member(&pool, account_id, "google_gmail", member1)
                    .await
                    .as_deref(),
                Some("member1@example.com")
            );

            // I7 no-fallback, direction 1: an account-wide lookup for Gmail's
            // OWN provider literal finds nothing — there is no NULL-owned
            // Gmail row to fall back to.
            assert_eq!(
                find_account_wide(&pool, account_id, "google_gmail").await,
                None
            );
            // I7 no-fallback, direction 2: member1's own per-owner lookup
            // against CALENDAR's provider literal finds nothing — Calendar's
            // row is NULL-owned, never member1's, so a per-member query can
            // never accidentally read the account-wide Calendar token.
            assert_eq!(
                find_member(&pool, account_id, "google", member1).await,
                None
            );
            // A member who never connected Gmail at all gets nothing either —
            // not member1's row, not an error.
            assert_eq!(
                find_member(&pool, account_id, "google_gmail", member2).await,
                None
            );

            teardown(&base, db, pool).await;
        }

        #[tokio::test]
        async fn list_and_delete_connections_are_owner_scoped_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping list_and_delete_connections_are_owner_scoped_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            set_test_kek();
            let db = "gws_g21_list_delete_owner_scoped";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member1 = seed_user(&pool).await;
            let member2 = seed_user(&pool).await;

            store(
                &pool,
                account_id,
                "google",
                TokenOwner::AccountWide,
                &calendar_bundle("acct@example.com"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member1),
                &gmail_bundle("member1@example.com"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member2),
                &gmail_bundle("member2@example.com"),
            )
            .await;

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

            // ── list_connections, as member1: sees the account-wide Calendar
            // row PLUS their own Gmail row — never member2's. ─────────────────
            let member1_ctx = UserContext {
                user_id: member1,
                account_id,
                role: "owner".to_string(),
                device_id: "test-device".to_string(),
            };
            let list_resp = list_connections(
                State(app_state.clone()),
                personal_tenant_for_test(),
                Extension(member1_ctx),
            )
            .await;
            assert_eq!(list_resp.status(), StatusCode::OK);
            let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            // `OAuthConnection` derives `Serialize` only (never `Deserialize`
            // — it's a one-way response type, and adding a derive to
            // production code just for a test is out of scope here), so this
            // reads the response body as generic JSON rather than the typed
            // struct.
            let conns: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let conns = conns.as_array().expect("connections list is a JSON array");
            assert_eq!(conns.len(), 2, "{conns:?}");
            assert!(conns.iter().any(|c| c["provider"] == "google"));
            let gmail_conn = conns
                .iter()
                .find(|c| c["provider"] == "google_gmail")
                .expect("member1's own gmail row must be listed");
            assert_eq!(
                gmail_conn["provider_workspace_id"].as_str(),
                Some("member1@example.com"),
                "must be member1's OWN row, never member2's"
            );

            // ── delete_connection("google_gmail"), as member1: removes ONLY
            // member1's row. ────────────────────────────────────────────────
            let member1_ctx = UserContext {
                user_id: member1,
                account_id,
                role: "owner".to_string(),
                device_id: "test-device".to_string(),
            };
            let delete_resp = delete_connection(
                State(app_state.clone()),
                personal_tenant_for_test(),
                Extension(member1_ctx),
                Path("google_gmail".to_string()),
            )
            .await;
            assert_eq!(delete_resp.status(), StatusCode::OK);

            let remaining: Vec<(String, Option<Uuid>)> = sqlx::query_as(
                "SELECT provider, owner_user_id FROM oauth_tokens \
                 WHERE account_id = $1 ORDER BY provider, owner_user_id NULLS FIRST",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                remaining.len(),
                2,
                "Calendar (account-wide) and member2's Gmail row must survive: {remaining:?}"
            );
            assert!(remaining.iter().any(|(p, o)| p == "google" && o.is_none()));
            assert!(
                remaining
                    .iter()
                    .any(|(p, o)| p == "google_gmail" && *o == Some(member2)),
                "member2's row must be untouched by member1's disconnect: {remaining:?}"
            );
            assert!(
                !remaining.iter().any(|(_, o)| *o == Some(member1)),
                "member1's own row must be gone: {remaining:?}"
            );

            teardown(&base, db, pool).await;
        }

        /// A Calendar credential a sync pass marked revoked is listed WITH
        /// its mark — the connectors page's reconnect notice reads it off
        /// this response — and a reconnect (the same upsert the callback
        /// runs) lists it live again. The personal account-wide row is the
        /// case `credential_health::notify_backer_lost_after_invalid` never
        /// reaches (no shared capability), so this is its only surface.
        #[tokio::test]
        async fn a_revoked_calendar_connection_is_listed_with_its_mark_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping a_revoked_calendar_connection_is_listed_with_its_mark_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            set_test_kek();
            let db = "gcal_list_revoked_mark";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            store(&pool, account_id, "google", TokenOwner::AccountWide, &calendar_bundle("acct@example.com")).await;
            crate::connectors::credential_health::mark_credential_invalid(
                &pool,
                account_id,
                TokenOwner::AccountWide,
                "google",
                crate::connectors::CredentialDeathReason::GoogleInvalidGrant,
                None,
            )
            .await
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let list = |app_state: Arc<AppState>| async move {
                let ctx = UserContext {
                    user_id: owner,
                    account_id,
                    role: "owner".to_string(),
                    device_id: "test-device".to_string(),
                };
                let resp = list_connections(State(app_state), personal_tenant_for_test(), Extension(ctx)).await;
                assert_eq!(resp.status(), StatusCode::OK);
                let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
                let conns: serde_json::Value = serde_json::from_slice(&body).unwrap();
                conns.as_array().unwrap().iter().find(|c| c["provider"] == "google").cloned().unwrap()
            };

            let calendar = list(app_state.clone()).await;
            assert_eq!(calendar["credential_invalid_reason"], "google_invalid_grant", "{calendar}");
            assert!(calendar["credential_invalid_at"].is_string(), "{calendar}");

            store(&pool, account_id, "google", TokenOwner::AccountWide, &calendar_bundle("acct@example.com")).await;
            let calendar = list(app_state.clone()).await;
            assert!(calendar["credential_invalid_at"].is_null(), "a reconnect lists it live: {calendar}");
            assert!(calendar["credential_invalid_reason"].is_null(), "{calendar}");

            teardown(&base, db, pool).await;
        }

        /// The first row per provider — what the connectors page reads for
        /// the reconnect notice — is the row a sync actually loads: the
        /// caller's OWN `Member` row before a legacy account-wide one, and
        /// the newest row before a revoked one a reconnect under a different
        /// provider account left behind. Oldest-first (the old order) put the
        /// wrong row first in both cases: the notice never showed for the
        /// member's dead token, and never cleared after the reconnect.
        #[tokio::test]
        async fn the_first_listed_row_per_provider_is_the_one_a_sync_loads_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping the_first_listed_row_per_provider_is_the_one_a_sync_loads_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            set_test_kek();
            let db = "oauth_list_row_precedence";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let first_row = |app_state: Arc<AppState>, provider: &'static str| async move {
                let ctx = UserContext {
                    user_id: owner,
                    account_id,
                    role: "owner".to_string(),
                    device_id: "test-device".to_string(),
                };
                let resp = list_connections(State(app_state), personal_tenant_for_test(), Extension(ctx)).await;
                assert_eq!(resp.status(), StatusCode::OK);
                let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
                let conns: serde_json::Value = serde_json::from_slice(&body).unwrap();
                conns.as_array().unwrap().iter().find(|c| c["provider"] == provider).cloned().unwrap()
            };
            let mark = |owner: TokenOwner, provider: &'static str| {
                let pool = pool.clone();
                async move {
                    crate::connectors::credential_health::mark_credential_invalid(
                        &pool,
                        account_id,
                        owner,
                        provider,
                        crate::connectors::CredentialDeathReason::MicrosoftConsentRevoked,
                        None,
                    )
                    .await
                    .unwrap()
                }
            };

            // 1. A legacy, live account-wide row created FIRST, then the
            //    caller's own member row — the one a sync loads — revoked.
            store(&pool, account_id, "microsoft", TokenOwner::AccountWide, &calendar_bundle("legacy@example.com")).await;
            store(&pool, account_id, "microsoft", TokenOwner::Member(owner), &calendar_bundle("me@example.com")).await;
            assert_eq!(mark(TokenOwner::Member(owner), "microsoft").await, 1);
            let row = first_row(app_state.clone(), "microsoft").await;
            assert_eq!(row["provider_workspace_id"], "me@example.com", "{row}");
            assert!(row["credential_invalid_at"].is_string(), "the member's dead token shows: {row}");

            // 2. A revoked row, then a reconnect under a DIFFERENT Google
            //    account (a new row — the unique key includes the workspace
            //    id): the newer, live row is the one listed first.
            store(&pool, account_id, "google", TokenOwner::AccountWide, &calendar_bundle("old@example.com")).await;
            assert_eq!(mark(TokenOwner::AccountWide, "google").await, 1);
            store(&pool, account_id, "google", TokenOwner::AccountWide, &calendar_bundle("new@example.com")).await;
            let row = first_row(app_state.clone(), "google").await;
            assert_eq!(row["provider_workspace_id"], "new@example.com", "{row}");
            assert!(row["credential_invalid_at"].is_null(), "the reconnect clears the notice: {row}");

            teardown(&base, db, pool).await;
        }

        /// gsuite Train G2 PR 6: `list_connections`/`delete_connection` — the
        /// REAL, unmodified production handlers, called directly with an ORG
        /// `Tenant` (`org_tenant_for_test`) this time. Covers the full matrix
        /// CodeRabbit rounds 1 and 2 on #695 narrowed this down to:
        ///
        /// * BOTH flags off — `list_connections` 403s (the route-level gate,
        ///   unchanged); `delete_connection` still succeeds — CodeRabbit
        ///   round 2: disconnect must never depend on either connector flag
        ///   (see that function's own "no connector-flag gate here at all"
        ///   comment) — an org member must always be able to revoke a token
        ///   that already exists, flag or no flag.
        /// * Slack ON, Gmail OFF — the route is reachable (via Slack), but
        ///   the caller's Gmail row must be FILTERED OUT of the list (see
        ///   `list_connections`' own `slack_visible`/`gmail_visible`
        ///   comment for the exact leak this closes) — yet `delete_connection`
        ///   still removes it, proving "hidden from the list" and
        ///   "revocable" are independent.
        /// * Gmail ON (Slack irrelevant here) — the row is listed and
        ///   deletable, the ordinary case.
        ///
        /// (`google_org_callback_allowed`'s own tests, above `mod
        /// google_token_isolation_pg`, cover `callback`'s half; `routes::
        /// connectors`' own pg tests cover `ingest_selected`'s half.)
        #[tokio::test]
        async fn org_gmail_list_and_delete_connections_gated_on_flag_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping org_gmail_list_and_delete_connections_gated_on_flag_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            let _guard = super::GOOGLE_SURFACE_ENV_LOCK.lock().await;
            set_test_kek();
            let db = "gws_g26_org_gmail_list_delete";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member = seed_user(&pool).await;
            let member_ctx = UserContext {
                user_id: member,
                account_id,
                role: "member".to_string(),
                device_id: "test-device".to_string(),
            };

            let prior_slack = std::env::var("ORG_SLACK_OAUTH_ENABLED").ok();
            let prior_gmail = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();

            // ── BOTH flags off: list refuses the whole route, delete still
            // revokes the caller's own row regardless (CodeRabbit round 2) ──
            // SAFETY: `_guard` (held above, `super::GOOGLE_SURFACE_ENV_LOCK`)
            // serializes every test in this module that mutates
            // `ORG_SLACK_OAUTH_ENABLED`/`ORG_GOOGLE_GMAIL_ENABLED`, so no
            // other test can observe or race this process-global env write.
            unsafe {
                std::env::remove_var("ORG_SLACK_OAUTH_ENABLED");
                std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED");
            }
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member),
                &gmail_bundle("orgmember@example.com"),
            )
            .await;
            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let list_resp = list_connections(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
            )
            .await;
            assert_eq!(
                list_resp.status(),
                StatusCode::FORBIDDEN,
                "org + both flags off must refuse list_connections at the route level"
            );
            let delete_resp = delete_connection(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
                Path("google_gmail".to_string()),
            )
            .await;
            assert_eq!(
                delete_resp.status(),
                StatusCode::OK,
                "delete_connection must NEVER depend on a connector flag — an existing row must \
                 always be revocable, even with every flag off"
            );
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
                    .bind(account_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                remaining, 0,
                "the row must actually be gone, not just 200 OK"
            );

            // ── Slack ON, Gmail OFF: route reachable via Slack, but the
            // Gmail row must be HIDDEN from the list — yet still deletable
            // (CodeRabbit round 2's exact leak scenario). Fresh `AppState`
            // each phase: `ConnectorFlagCache` is per-`AppState`, 5-minute
            // TTL — reusing one across a flag flip would read a stale
            // cached decision, a test-harness artifact, not a production
            // bug (`resolve`'s own doc comment).
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member),
                &gmail_bundle("orgmember@example.com"),
            )
            .await;
            // SAFETY: see the "BOTH flags off" phase's own SAFETY comment
            // above — same held `_guard` covers this mutation too.
            unsafe {
                std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
                std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED");
            }
            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let list_resp = list_connections(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
            )
            .await;
            assert_eq!(
                list_resp.status(),
                StatusCode::OK,
                "org + Slack on must allow the ROUTE through"
            );
            let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let conns: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let conns = conns.as_array().expect("connections list is a JSON array");
            assert!(
                conns.is_empty(),
                "the Gmail row must be filtered OUT while ORG_GOOGLE_GMAIL_ENABLED is off, even \
                 though the route itself is reachable via the Slack flag: {conns:?}"
            );
            let delete_resp = delete_connection(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
                Path("google_gmail".to_string()),
            )
            .await;
            assert_eq!(
                delete_resp.status(),
                StatusCode::OK,
                "a row hidden from the list must still be directly revocable"
            );
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
                    .bind(account_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(remaining, 0, "the hidden row must actually be gone");

            // ── Gmail ON: the ordinary case — visible and deletable ─────────
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member),
                &gmail_bundle("orgmember@example.com"),
            )
            .await;
            // SAFETY: see the "BOTH flags off" phase's own SAFETY comment
            // above — same held `_guard` covers this mutation too.
            unsafe {
                std::env::remove_var("ORG_SLACK_OAUTH_ENABLED");
                std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
            }
            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let list_resp = list_connections(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
            )
            .await;
            assert_eq!(
                list_resp.status(),
                StatusCode::OK,
                "org + flag on must allow list_connections"
            );
            let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let conns: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let conns = conns.as_array().expect("connections list is a JSON array");
            assert_eq!(conns.len(), 1, "{conns:?}");
            assert_eq!(conns[0]["provider"], "google_gmail");

            let delete_resp = delete_connection(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
                Path("google_gmail".to_string()),
            )
            .await;
            assert_eq!(
                delete_resp.status(),
                StatusCode::OK,
                "org + flag on must allow delete_connection"
            );
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
                    .bind(account_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(remaining, 0, "the caller's own Gmail row must be gone");

            match prior_slack {
                Some(v) => unsafe { std::env::set_var("ORG_SLACK_OAUTH_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_SLACK_OAUTH_ENABLED") },
            }
            match prior_gmail {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
            }
            drop(_guard);
            teardown(&base, db, pool).await;
        }

        /// gsuite Train G4 PR 6 ("G4.6"): the identical CodeRabbit round-2 leak
        /// `org_gmail_list_and_delete_connections_gated_on_flag_pg` closes for
        /// Gmail, proven for Chat — an org reachable via Slack alone must NOT
        /// list a `google_chat` row as if Chat were enabled, but must still be
        /// able to revoke it directly.
        #[tokio::test]
        async fn org_chat_list_and_delete_connections_gated_on_flag_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping org_chat_list_and_delete_connections_gated_on_flag_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            let _guard = super::GOOGLE_SURFACE_ENV_LOCK.lock().await;
            set_test_kek();
            let db = "gws_g46_org_chat_list_delete";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member = seed_user(&pool).await;
            let member_ctx = UserContext {
                user_id: member,
                account_id,
                role: "member".to_string(),
                device_id: "test-device".to_string(),
            };

            let prior_slack = std::env::var("ORG_SLACK_OAUTH_ENABLED").ok();
            let prior_chat = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();

            // ── Slack ON, Chat OFF: route reachable via Slack, but the Chat
            // row must be HIDDEN from the list — yet still deletable. ───────
            store(
                &pool,
                account_id,
                "google_chat",
                TokenOwner::Member(member),
                &chat_bundle("orgmember"),
            )
            .await;
            // SAFETY: `_guard` (held above, `super::GOOGLE_SURFACE_ENV_LOCK`)
            // serializes every test in this module that mutates
            // `ORG_SLACK_OAUTH_ENABLED`/`ORG_GOOGLE_CHAT_ENABLED`.
            unsafe {
                std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
                std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED");
            }
            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let list_resp = list_connections(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
            )
            .await;
            assert_eq!(
                list_resp.status(),
                StatusCode::OK,
                "org + Slack on must allow the ROUTE through"
            );
            let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let conns: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let conns = conns.as_array().expect("connections list is a JSON array");
            assert!(
                conns.is_empty(),
                "the Chat row must be filtered OUT while ORG_GOOGLE_CHAT_ENABLED is off, even \
                 though the route itself is reachable via the Slack flag: {conns:?}"
            );
            let delete_resp = delete_connection(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
                Path("google_chat".to_string()),
            )
            .await;
            assert_eq!(
                delete_resp.status(),
                StatusCode::OK,
                "a row hidden from the list must still be directly revocable"
            );
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
                    .bind(account_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(remaining, 0, "the hidden row must actually be gone");

            // ── Chat ON: the ordinary case — visible and deletable ──────────
            store(
                &pool,
                account_id,
                "google_chat",
                TokenOwner::Member(member),
                &chat_bundle("orgmember"),
            )
            .await;
            // SAFETY: see the phase above's own SAFETY comment — same held
            // `_guard` covers this mutation too.
            unsafe {
                std::env::remove_var("ORG_SLACK_OAUTH_ENABLED");
                std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true");
            }
            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let list_resp = list_connections(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
            )
            .await;
            assert_eq!(
                list_resp.status(),
                StatusCode::OK,
                "org + flag on must allow list_connections"
            );
            let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let conns: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let conns = conns.as_array().expect("connections list is a JSON array");
            assert_eq!(conns.len(), 1, "{conns:?}");
            assert_eq!(conns[0]["provider"], "google_chat");

            let delete_resp = delete_connection(
                State(app_state.clone()),
                org_tenant_for_test(),
                Extension(member_ctx.clone()),
                Path("google_chat".to_string()),
            )
            .await;
            assert_eq!(
                delete_resp.status(),
                StatusCode::OK,
                "org + flag on must allow delete_connection"
            );
            let remaining: i64 =
                sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
                    .bind(account_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(remaining, 0, "the caller's own Chat row must be gone");

            match prior_slack {
                Some(v) => unsafe { std::env::set_var("ORG_SLACK_OAUTH_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_SLACK_OAUTH_ENABLED") },
            }
            match prior_chat {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
            }
            drop(_guard);
            teardown(&base, db, pool).await;
        }

        #[tokio::test]
        async fn delete_connection_does_not_touch_another_owners_install_row_sharing_a_workspace_id_pg(
        ) {
            // CodeRabbit round-2 fix on this PR: `connector_installs`' own
            // per-owner partial unique index (`uq_connector_installs_per_owner`)
            // allows two DIFFERENT owners to each hold a row for the SAME
            // `(provider, external_workspace_id, account_id)` triple — so two
            // members whose Gmail connections happen to land on the identical
            // `provider_workspace_id` (deliberately forced here, whatever the
            // real-world trigger) must NOT interfere with each other's
            // `connector_installs` row, exactly as they already don't for
            // `oauth_tokens`. Seeds both tables the way `callback` really
            // writes them (`store_with_install`), disconnects member1's own
            // Gmail connection, and asserts member2's token AND install row
            // both survive untouched.
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping delete_connection_does_not_touch_another_owners_install_row_sharing_a_workspace_id_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "gws_g21_install_owner_scoped_delete";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member1 = seed_user(&pool).await;
            let member2 = seed_user(&pool).await;

            // Both members' Gmail bundles share the SAME workspace id
            // (`provider_workspace_id`) — the collision `connector_installs`'
            // per-owner unique index structurally allows and this test
            // exercises. `store_with_install` writes both `oauth_tokens` AND
            // `connector_installs` in one transaction, matching `callback`.
            let shared_workspace = "shared@example.com";
            store_with_install(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member1),
                &gmail_bundle(shared_workspace),
            )
            .await;
            store_with_install(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member2),
                &gmail_bundle(shared_workspace),
            )
            .await;

            // Sanity: two token rows AND two install rows exist before the
            // disconnect, both keyed on the identical workspace id.
            let token_count_before: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM oauth_tokens \
                 WHERE account_id = $1 AND provider = 'google_gmail' AND provider_workspace_id = $2",
            )
            .bind(account_id)
            .bind(shared_workspace)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                token_count_before, 2,
                "sanity: both members' tokens must exist first"
            );
            let install_count_before: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM connector_installs \
                 WHERE account_id = $1 AND provider = 'google_gmail' AND external_workspace_id = $2",
            )
            .bind(account_id)
            .bind(shared_workspace)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                install_count_before, 2,
                "sanity: both members' install rows must exist first"
            );

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let member1_ctx = UserContext {
                user_id: member1,
                account_id,
                role: "owner".to_string(),
                device_id: "test-device".to_string(),
            };
            let delete_resp = delete_connection(
                State(app_state),
                personal_tenant_for_test(),
                Extension(member1_ctx),
                Path("google_gmail".to_string()),
            )
            .await;
            assert_eq!(delete_resp.status(), StatusCode::OK);

            // member1's own rows are gone from BOTH tables.
            let member1_token: Option<Uuid> = sqlx::query_scalar(
                "SELECT owner_user_id FROM oauth_tokens \
                 WHERE account_id = $1 AND provider = 'google_gmail' AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(member1)
            .fetch_optional(&pool)
            .await
            .unwrap();
            assert_eq!(member1_token, None, "member1's own token row must be gone");
            let member1_install: Option<Uuid> = sqlx::query_scalar(
                "SELECT owner_user_id FROM connector_installs \
                 WHERE account_id = $1 AND provider = 'google_gmail' AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(member1)
            .fetch_optional(&pool)
            .await
            .unwrap();
            assert_eq!(
                member1_install, None,
                "member1's own install row must be gone"
            );

            // member2's rows — SAME workspace id, DIFFERENT owner — survive in
            // BOTH tables untouched. This is the exact assertion the missing
            // `owner_user_id` scoping on the `connector_installs` DELETE would
            // have failed before this fix.
            let member2_token: Option<Uuid> = sqlx::query_scalar(
                "SELECT owner_user_id FROM oauth_tokens \
                 WHERE account_id = $1 AND provider = 'google_gmail' AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(member2)
            .fetch_optional(&pool)
            .await
            .unwrap();
            assert_eq!(
                member2_token,
                Some(member2),
                "member2's token row must survive member1's disconnect"
            );
            let member2_install: Option<Uuid> = sqlx::query_scalar(
                "SELECT owner_user_id FROM connector_installs \
                 WHERE account_id = $1 AND provider = 'google_gmail' AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(member2)
            .fetch_optional(&pool)
            .await
            .unwrap();
            assert_eq!(
                member2_install,
                Some(member2),
                "member2's install row must survive member1's disconnect — this is the \
                 CodeRabbit round-2 regression: an unscoped connector_installs DELETE would \
                 have removed it too, since it shares member1's workspace id"
            );

            teardown(&base, db, pool).await;
        }

        /// End-to-end proof of the Google disconnect purge's REAL capture
        /// path (CodeRabbit round 2 on PR #690, Major: `purge_not_after` must
        /// be sourced from the SAME clock as `google_watch_channels.
        /// updated_at`, not an app-process `Utc::now()` — see
        /// `connectors::google_disconnect`'s own module doc for the full
        /// mechanism). Every OTHER test in this module passes
        /// `personal_tenant_for_test()`, whose `pool` field is `None` — so
        /// `delete_connection`'s Google purge block has never actually run
        /// through any of them; they only ever exercise the `oauth_tokens`/
        /// `connector_installs` half. This test builds a `Tenant` with a REAL
        /// `pool` (mirroring what a genuine Personal-plane request carries —
        /// see `connectors::google_disconnect`'s own doc on why `tenant_pool`
        /// and the control pool are the same physical server on the Personal
        /// plane Google is scoped to today) so the purge block's `Some(tenant_pool)`
        /// arm — and therefore the real inline `SELECT now()` capture inside
        /// it — actually executes, not just the code paths this module's
        /// other tests reach.
        #[tokio::test]
        async fn delete_connection_google_gmail_purges_the_watch_channel_through_the_real_capture_path_pg(
        ) {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping delete_connection_google_gmail_purges_the_watch_channel_through_the_real_capture_path_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "gws_g25_purge_real_capture_path";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member = seed_user(&pool).await;
            let other_member = seed_user(&pool).await;
            let email = "member@example.com";

            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(member),
                &gmail_bundle(email),
            )
            .await;

            // The row the purge must remove — `updated_at` defaults to the
            // DATABASE's own `now()` at insert time, exactly like a real
            // `ensure_watch` upsert would set it.
            sqlx::query(
                "INSERT INTO google_watch_channels \
                     (account_id, surface, resource_id, owner_user_id, expires_at) \
                 VALUES ($1, 'gmail', $2, $3, now() + interval '1 day')",
            )
            .bind(account_id)
            .bind(email)
            .bind(member)
            .execute(&pool)
            .await
            .unwrap();
            // An unrelated member's watch channel — must survive (owner
            // scoping), proving this end-to-end path doesn't just delete
            // everything once it runs for real.
            sqlx::query(
                "INSERT INTO google_watch_channels \
                     (account_id, surface, resource_id, owner_user_id, expires_at) \
                 VALUES ($1, 'gmail', 'other@example.com', $2, now() + interval '1 day')",
            )
            .bind(account_id)
            .bind(other_member)
            .execute(&pool)
            .await
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let member_ctx = UserContext {
                user_id: member,
                account_id,
                role: "owner".to_string(),
                device_id: "test-device".to_string(),
            };
            // A REAL pool, not `personal_tenant_for_test()`'s `pool: None` —
            // see this test's own doc comment for why that distinction is
            // exactly what this test exists to cover.
            let tenant_with_pool = Tenant {
                store: None,
                pool: Some(pool.clone()),
                decision: None,
                class: crate::route_class::RouteClass::Other,
            };
            let delete_resp = delete_connection(
                State(app_state),
                tenant_with_pool,
                Extension(member_ctx),
                Path("google_gmail".to_string()),
            )
            .await;
            assert_eq!(delete_resp.status(), StatusCode::OK);

            let member_channel_gone: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM google_watch_channels \
                 WHERE account_id = $1 AND resource_id = $2",
            )
            .bind(account_id)
            .bind(email)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                member_channel_gone, 0,
                "the disconnecting member's own watch channel must be purged through the REAL \
                 delete_connection call — the DB-sourced purge_not_after capture must not \
                 spuriously exclude a row that is genuinely stale by the tenant pool's own clock"
            );

            let other_channel_survives: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM google_watch_channels \
                 WHERE account_id = $1 AND resource_id = 'other@example.com'",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                other_channel_survives, 1,
                "another member's watch channel must survive this disconnect untouched"
            );

            teardown(&base, db, pool).await;
        }

        // ── gsuite Train G4 PR 1 ("G4.1"): conformance test 1 — cross-member
        // Chat token isolation (contract I7) ──────────────────────────────
        //
        // Against a REAL Postgres database: proves the schema-level
        // invariant G4.1's 501 lift rests on — a Calendar `AccountWide` row,
        // a single member's Gmail `Member` row, and TWO members' Chat
        // `Member` rows all coexist under the SAME `(account_id, provider)`
        // shape (each surface's own distinct `oauth_tokens.provider`
        // literal) as four rows, without collision — and that
        // `load_creds_bg` (`routes::connectors`, the exact owner-exact query
        // shape the request-path `load_creds` also uses — see that
        // function's own doc comment) loading Member(A)'s Chat token can
        // never return Member(B)'s, in either direction.
        #[tokio::test]
        async fn calendar_gmail_and_two_members_chat_tokens_coexist_as_four_rows_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping calendar_gmail_and_two_members_chat_tokens_coexist_as_four_rows_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "gws_g41_four_row_coexist";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let gmail_member = seed_user(&pool).await;
            let chat_member_a = seed_user(&pool).await;
            let chat_member_b = seed_user(&pool).await;

            store(
                &pool,
                account_id,
                "google",
                TokenOwner::AccountWide,
                &calendar_bundle("acct@example.com"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_gmail",
                TokenOwner::Member(gmail_member),
                &gmail_bundle("gmailmember@example.com"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_chat",
                TokenOwner::Member(chat_member_a),
                &chat_bundle("member-a"),
            )
            .await;
            store(
                &pool,
                account_id,
                "google_chat",
                TokenOwner::Member(chat_member_b),
                &chat_bundle("member-b"),
            )
            .await;

            // ── Four rows, no collision — one Calendar (AccountWide), one
            // Gmail (Member), two Chat (Member), each keeping its own
            // distinct `provider` literal (`Surface::token_provider`). ─────
            let rows: Vec<(String, Option<Uuid>, String)> = sqlx::query_as(
                "SELECT provider, owner_user_id, provider_workspace_id FROM oauth_tokens \
                 WHERE account_id = $1 ORDER BY provider, provider_workspace_id",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                rows.len(),
                4,
                "calendar + gmail member + two chat members must coexist: {rows:?}"
            );
            assert_eq!(
                rows[0],
                ("google".to_string(), None, "acct@example.com".to_string()),
                "Calendar stays the grandfathered literal, account-wide"
            );
            assert_eq!(
                rows[1].0, "google_chat",
                "google_chat sorts before google_gmail lexically: {rows:?}"
            );
            assert_eq!(
                rows[2].0, "google_chat",
                "both chat members' rows share the google_chat literal: {rows:?}"
            );
            assert_eq!(
                rows[3],
                (
                    "google_gmail".to_string(),
                    Some(gmail_member),
                    "gmailmember@example.com".to_string()
                )
            );
            let chat_owners: std::collections::HashSet<Option<Uuid>> = rows
                .iter()
                .filter(|(p, _, _)| p == "google_chat")
                .map(|(_, o, _)| *o)
                .collect();
            assert_eq!(
                chat_owners,
                std::collections::HashSet::from([Some(chat_member_a), Some(chat_member_b)]),
                "both chat members' rows must be Member-owned, distinctly: {rows:?}"
            );

            // ── I7: `load_creds_bg(Member(A))` never returns B's token, in
            // either direction — the real production credential-load query,
            // not a hand-rolled stand-in (`load_creds_bg`'s own doc comment:
            // same `begin_account_scoped` scoping and owner-exact predicate
            // as the request-path `load_creds`). ─────────────────────────
            // Personal-plane fixture: the DEK plane is the consumer one,
            // byte-identical to the pre-plane-parameter hardcoded
            // `TenantId::Consumer` resolution this test was written against.
            let dek_plane = crate::crypto::DekPlane::consumer(pool.clone());
            let creds_a = crate::routes::connectors::load_creds_bg(
                &pool,
                &dek_plane,
                account_id,
                "google_chat",
                TokenOwner::Member(chat_member_a),
            )
            .await
            .expect("load_creds_bg must succeed")
            .expect("member A's chat row must be found");
            assert_eq!(creds_a.access_token, "chat-access-member-a");
            assert_eq!(creds_a.provider_workspace_id, "workspace-member-a");

            let creds_b = crate::routes::connectors::load_creds_bg(
                &pool,
                &dek_plane,
                account_id,
                "google_chat",
                TokenOwner::Member(chat_member_b),
            )
            .await
            .expect("load_creds_bg must succeed")
            .expect("member B's chat row must be found");
            assert_eq!(creds_b.access_token, "chat-access-member-b");
            assert_ne!(
                creds_a.access_token, creds_b.access_token,
                "member A and member B must never resolve to the same token"
            );

            // A member with no chat row of their own — e.g. the Gmail-only
            // member — gets `Ok(None)`, never a cross-surface or
            // cross-member leak.
            let creds_gmail_member_as_chat = crate::routes::connectors::load_creds_bg(
                &pool,
                &dek_plane,
                account_id,
                "google_chat",
                TokenOwner::Member(gmail_member),
            )
            .await
            .expect("load_creds_bg must succeed even when nothing is found");
            assert!(
                creds_gmail_member_as_chat.is_none(),
                "a member with no chat connection of their own must get nothing, never \
                 another member's row"
            );

            // And an account-wide lookup under Chat's OWN provider literal
            // finds nothing either — there is no NULL-owned `google_chat`
            // row to fall back to (Chat is unconditionally per-member, see
            // `Surface::token_owner_for`).
            let creds_account_wide_chat = crate::routes::connectors::load_creds_bg(
                &pool,
                &dek_plane,
                account_id,
                "google_chat",
                TokenOwner::AccountWide,
            )
            .await
            .expect("load_creds_bg must succeed even when nothing is found");
            assert!(
                creds_account_wide_chat.is_none(),
                "there is no account-wide google_chat row to fall back to"
            );

            teardown(&base, db, pool).await;
        }

        // ── CodeRabbit round 1 on gsuite Train G4 PR 1 ("G4.1"): `authorize`
        // must never redirect an org-plane caller into a live Google
        // consent round trip that `callback`'s own org/provider gate is
        // guaranteed to refuse moments later (`refuse_doomed_org_authorize_
        // redirect`, this file). **Superseded by gsuite Train G4 PR 6
        // ("G4.6")**: `google_org_callback_allowed` now HAS a Chat arm
        // (`org_google_chat_enabled_for`), so an org-plane Chat authorize
        // request with its own flag on is no longer doomed — it is the exact
        // scenario `authorize_google_chat_org_state_redirects_when_its_own_
        // flag_is_on_pg` below now covers, mirroring Gmail's own regression
        // test. This test is renamed and re-pointed at the ONE case that
        // still IS doomed post-G4.6: the flag OFF for this org, so
        // `google_org_callback_allowed`'s Chat arm itself refuses — proving
        // the pre-redirect check still closes a live-Google-round-trip for
        // the flag-off case, not just the "no Chat arm at all" case G4.1
        // originally guarded against. ─────────────────────────────────────
        #[tokio::test]
        async fn authorize_google_chat_org_state_refused_before_redirect_when_flag_off_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping authorize_google_chat_org_state_refused_before_redirect_when_flag_off_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            let (_g1, _g2) = super::lock_google_surface_and_state_secret_env().await;
            set_test_kek();
            let db = "gws_g46_org_chat_flag_off_no_doomed_redirect";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            // An ORG account — `clerk_org_id` set — whose Chat flag resolves
            // OFF (no env default, no `org_connector_flags` override row).
            let org_id = format!("org_{}", Uuid::new_v4());
            let account_id: Uuid = sqlx::query_scalar(
                "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner)
            .bind(&org_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            let prior_chat_flag = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
            let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
            let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
            let secret = "g46-authorize-org-chat-flag-off-secret-0";
            // SAFETY: `_g1`/`_g2` (held above, `GOOGLE_SURFACE_ENV_LOCK` and
            // `OAUTH_STATE_SECRET_ENV_LOCK`) together serialize every test
            // that mutates `ORG_GOOGLE_CHAT_ENABLED`/`GOOGLE_CALENDAR_
            // CLIENT_ID`/`OAUTH_STATE_SECRET`, so no other test can observe
            // or race these process-global env writes.
            unsafe {
                // Deliberately OFF — the surface-flag gate on `authorize`
                // itself would already refuse this (`google_surface_enabled_
                // for`), but this test's whole point is the SEPARATE
                // pre-redirect org-gate check, so both must independently
                // refuse.
                std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED");
                std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
                std::env::set_var("OAUTH_STATE_SECRET", secret);
            }

            // A REAL, validly signed state declaring both a connecting user
            // id and the `chat` surface — exactly what `mint_state` would
            // have produced (its own else-branch gate is provider-blind on
            // Slack/Microsoft flags, not Chat's — see `reject_org_oauth_
            // management`'s own doc — so this is reachable in practice).
            let token = oauth_state::sign_state(
                secret,
                account_id,
                Utc::now().timestamp(),
                "g46-flag-off-doomed-redirect-nonce",
                Some(owner),
                Some("chat"),
            )
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let router = Router::new()
                .route("/v1/oauth/{provider}/authorize", get(authorize))
                .with_state(app_state);
            let req = HttpRequest::builder()
                .uri(format!(
                    "/v1/oauth/google/authorize?surface=chat&state={token}"
                ))
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            match prior_chat_flag {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
            }
            match prior_client_id {
                Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
                None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
            }
            match prior_secret {
                Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
                None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
            }

            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "an org-plane Chat authorize request must be refused BEFORE redirecting to \
                 Google — never a 302 — while ORG_GOOGLE_CHAT_ENABLED resolves off for this org"
            );

            teardown(&base, db, pool).await;
        }

        /// gsuite Train G4 PR 6 ("G4.6"): Chat's own real org flow (its flag
        /// on, `google_org_callback_allowed`'s new Chat arm allows it) must
        /// redirect — the org-plane gate lift's whole point. Mirrors
        /// `authorize_google_gmail_org_state_still_redirects_when_its_own_
        /// flag_is_on_pg` exactly, one surface later.
        #[tokio::test]
        async fn authorize_google_chat_org_state_redirects_when_its_own_flag_is_on_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping authorize_google_chat_org_state_redirects_when_its_own_flag_is_on_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            let (_g1, _g2) = super::lock_google_surface_and_state_secret_env().await;
            set_test_kek();
            let db = "gws_g46_org_chat_flag_on_redirects";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let org_id = format!("org_{}", Uuid::new_v4());
            let account_id: Uuid = sqlx::query_scalar(
                "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner)
            .bind(&org_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            let prior_chat_flag = std::env::var("ORG_GOOGLE_CHAT_ENABLED").ok();
            let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
            let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
            let secret = "g46-authorize-org-chat-still-works-secret-0";
            // SAFETY: `_g1`/`_g2` (held above, `GOOGLE_SURFACE_ENV_LOCK` and
            // `OAUTH_STATE_SECRET_ENV_LOCK`) together serialize every test
            // that mutates `ORG_GOOGLE_CHAT_ENABLED`/`GOOGLE_CALENDAR_
            // CLIENT_ID`/`OAUTH_STATE_SECRET`, so no other test can observe
            // or race these process-global env writes.
            unsafe {
                std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", "true");
                std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
                std::env::set_var("OAUTH_STATE_SECRET", secret);
            }

            let token = oauth_state::sign_state(
                secret,
                account_id,
                Utc::now().timestamp(),
                "g46-org-chat-still-works-nonce",
                Some(owner),
                Some("chat"),
            )
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let router = Router::new()
                .route("/v1/oauth/{provider}/authorize", get(authorize))
                .with_state(app_state);
            let req = HttpRequest::builder()
                .uri(format!(
                    "/v1/oauth/google/authorize?surface=chat&state={token}"
                ))
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            match prior_chat_flag {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CHAT_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_CHAT_ENABLED") },
            }
            match prior_client_id {
                Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
                None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
            }
            match prior_secret {
                Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
                None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
            }

            assert_eq!(
                resp.status(),
                StatusCode::TEMPORARY_REDIRECT,
                "Chat's own real org flow (its own flag on, callback's own Chat arm allows it) \
                 must redirect — the gsuite Train G4 PR 6 org-plane gate lift's whole point"
            );

            teardown(&base, db, pool).await;
        }

        // ── Google Calendar org lift: `authorize`'s Calendar pre-redirect org gate ──

        /// An ORG account whose `google_calendar` flag resolves OFF is refused BEFORE any redirect to Google (a doomed consent round trip `callback` would 403).
        #[tokio::test]
        async fn authorize_google_calendar_org_state_refused_before_redirect_when_flag_off_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping authorize_google_calendar_org_state_refused_before_redirect_when_flag_off_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            let (_g1, _g2) = super::lock_google_surface_and_state_secret_env().await;
            set_test_kek();
            let db = "gcal_org_authorize_flag_off";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let org_id = format!("org_{}", Uuid::new_v4());
            let account_id: Uuid = sqlx::query_scalar(
                "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner)
            .bind(&org_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            let prior_flag = std::env::var("ORG_GOOGLE_CALENDAR_ENABLED").ok();
            let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
            let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
            let secret = "gcal-org-authorize-gate-secret-000000";
            // SAFETY: `_g1`/`_g2` serialize every test that mutates these
            // process-global env vars (`GOOGLE_SURFACE_ENV_LOCK`,
            // `OAUTH_STATE_SECRET_ENV_LOCK`).
            unsafe {
                std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED");
                std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
                std::env::set_var("OAUTH_STATE_SECRET", secret);
            }

            // A REAL, validly signed state exactly as `mint_state` produces it
            // for `?surface=calendar` (surface claim + connecting user id).
            let token = oauth_state::sign_state(
                secret,
                account_id,
                Utc::now().timestamp(),
                "gcal-org-authorize-gate-nonce",
                Some(owner),
                Some("calendar"),
            )
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let router = Router::new()
                .route("/v1/oauth/{provider}/authorize", get(authorize))
                .with_state(app_state);
            let req = HttpRequest::builder()
                .uri(format!("/v1/oauth/google/authorize?surface=calendar&state={token}"))
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            match prior_flag {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
            }
            match prior_client_id {
                Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
                None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
            }
            match prior_secret {
                Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
                None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
            }

            assert_eq!(resp.status(), StatusCode::FORBIDDEN, "an org-plane Calendar authorize must be refused (never a 302) while ORG_GOOGLE_CALENDAR_ENABLED resolves off for the org");

            teardown(&base, db, pool).await;
        }

        /// The same org with its Calendar flag ON redirects to Google — the lift's whole point.
        #[tokio::test]
        async fn authorize_google_calendar_org_state_redirects_when_its_own_flag_is_on_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping authorize_google_calendar_org_state_redirects_when_its_own_flag_is_on_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            let (_g1, _g2) = super::lock_google_surface_and_state_secret_env().await;
            set_test_kek();
            let db = "gcal_org_authorize_flag_on";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let org_id = format!("org_{}", Uuid::new_v4());
            let account_id: Uuid = sqlx::query_scalar(
                "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner)
            .bind(&org_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            let prior_flag = std::env::var("ORG_GOOGLE_CALENDAR_ENABLED").ok();
            let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
            let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
            let secret = "gcal-org-authorize-gate-secret-000000";
            // SAFETY: `_g1`/`_g2` serialize every test that mutates these
            // process-global env vars (`GOOGLE_SURFACE_ENV_LOCK`,
            // `OAUTH_STATE_SECRET_ENV_LOCK`).
            unsafe {
                std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", "true");
                std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
                std::env::set_var("OAUTH_STATE_SECRET", secret);
            }

            // A REAL, validly signed state exactly as `mint_state` produces it
            // for `?surface=calendar` (surface claim + connecting user id).
            let token = oauth_state::sign_state(
                secret,
                account_id,
                Utc::now().timestamp(),
                "gcal-org-authorize-gate-nonce",
                Some(owner),
                Some("calendar"),
            )
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let router = Router::new()
                .route("/v1/oauth/{provider}/authorize", get(authorize))
                .with_state(app_state);
            let req = HttpRequest::builder()
                .uri(format!("/v1/oauth/google/authorize?surface=calendar&state={token}"))
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            match prior_flag {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
            }
            match prior_client_id {
                Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
                None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
            }
            match prior_secret {
                Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
                None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
            }

            assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT, "an org-plane Calendar authorize with the org's own flag on must redirect");

            teardown(&base, db, pool).await;
        }

        /// A surface-less authorize (the legacy Calendar spelling) under an org with the flag OFF is refused identically — the gate cannot be bypassed by omitting `?surface=`.
        #[tokio::test]
        async fn authorize_google_calendar_org_state_without_a_surface_param_is_gated_the_same_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping authorize_google_calendar_org_state_without_a_surface_param_is_gated_the_same_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            let (_g1, _g2) = super::lock_google_surface_and_state_secret_env().await;
            set_test_kek();
            let db = "gcal_org_authorize_nosurface";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let org_id = format!("org_{}", Uuid::new_v4());
            let account_id: Uuid = sqlx::query_scalar(
                "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner)
            .bind(&org_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            let prior_flag = std::env::var("ORG_GOOGLE_CALENDAR_ENABLED").ok();
            let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
            let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
            let secret = "gcal-org-authorize-gate-secret-000000";
            // SAFETY: `_g1`/`_g2` serialize every test that mutates these
            // process-global env vars (`GOOGLE_SURFACE_ENV_LOCK`,
            // `OAUTH_STATE_SECRET_ENV_LOCK`).
            unsafe {
                std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED");
                std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
                std::env::set_var("OAUTH_STATE_SECRET", secret);
            }

            // A REAL, validly signed state exactly as `mint_state` produces it
            // for `?surface=calendar` (surface claim + connecting user id).
            let token = oauth_state::sign_state(
                secret,
                account_id,
                Utc::now().timestamp(),
                "gcal-org-authorize-gate-nonce",
                Some(owner),
                Some("calendar"),
            )
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let router = Router::new()
                .route("/v1/oauth/{provider}/authorize", get(authorize))
                .with_state(app_state);
            let req = HttpRequest::builder()
                .uri(format!("/v1/oauth/google/authorize?state={token}"))
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            match prior_flag {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
            }
            match prior_client_id {
                Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
                None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
            }
            match prior_secret {
                Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
                None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
            }

            assert_eq!(resp.status(), StatusCode::FORBIDDEN, "omitting ?surface= must not bypass the org Calendar gate");

            teardown(&base, db, pool).await;
        }

        /// A PERSONAL account's Calendar connect is byte-identical: it redirects with the org flag off (Calendar stays grandfathered ON for the personal plane).
        #[tokio::test]
        async fn authorize_google_calendar_personal_state_still_redirects_with_the_org_flag_off_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping authorize_google_calendar_personal_state_still_redirects_with_the_org_flag_off_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            let (_g1, _g2) = super::lock_google_surface_and_state_secret_env().await;
            set_test_kek();
            let db = "gcal_org_authorize_personal";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id: Uuid =
                sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                    .bind(owner)
                    .fetch_one(&pool)
                    .await
                    .unwrap();

            let prior_flag = std::env::var("ORG_GOOGLE_CALENDAR_ENABLED").ok();
            let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
            let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
            let secret = "gcal-org-authorize-gate-secret-000000";
            // SAFETY: `_g1`/`_g2` serialize every test that mutates these
            // process-global env vars (`GOOGLE_SURFACE_ENV_LOCK`,
            // `OAUTH_STATE_SECRET_ENV_LOCK`).
            unsafe {
                std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED");
                std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
                std::env::set_var("OAUTH_STATE_SECRET", secret);
            }

            // A REAL, validly signed state exactly as `mint_state` produces it
            // for `?surface=calendar` (surface claim + connecting user id).
            let token = oauth_state::sign_state(
                secret,
                account_id,
                Utc::now().timestamp(),
                "gcal-org-authorize-gate-nonce",
                Some(owner),
                Some("calendar"),
            )
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let router = Router::new()
                .route("/v1/oauth/{provider}/authorize", get(authorize))
                .with_state(app_state);
            let req = HttpRequest::builder()
                .uri(format!("/v1/oauth/google/authorize?surface=calendar&state={token}"))
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            match prior_flag {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
            }
            match prior_client_id {
                Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
                None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
            }
            match prior_secret {
                Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
                None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
            }

            assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT, "a personal Calendar authorize must be unaffected by the org flag");

            teardown(&base, db, pool).await;
        }

        /// Regression coverage for the fix above: Gmail's own REAL org flow
        /// (its flag on, `google_org_callback_allowed`'s existing Gmail arm
        /// allows it) must be completely unaffected by the new pre-redirect
        /// check — it consults the SAME function `callback` already uses, so
        /// a flow callback would accept must still redirect.
        #[tokio::test]
        async fn authorize_google_gmail_org_state_still_redirects_when_its_own_flag_is_on_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping authorize_google_gmail_org_state_still_redirects_when_its_own_flag_is_on_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            let (_g1, _g2) = super::lock_google_surface_and_state_secret_env().await;
            set_test_kek();
            let db = "gws_g41_org_gmail_still_redirects";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let org_id = format!("org_{}", Uuid::new_v4());
            let account_id: Uuid = sqlx::query_scalar(
                "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
            )
            .bind(owner)
            .bind(&org_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            let prior_gmail_flag = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
            let prior_client_id = std::env::var("GOOGLE_CALENDAR_CLIENT_ID").ok();
            let prior_secret = std::env::var("OAUTH_STATE_SECRET").ok();
            let secret = "g41-authorize-org-gmail-still-works-secret-0";
            // SAFETY: `_g1`/`_g2` (held above, `GOOGLE_SURFACE_ENV_LOCK` and
            // `OAUTH_STATE_SECRET_ENV_LOCK`) together serialize every test
            // that mutates `ORG_GOOGLE_GMAIL_ENABLED`/`GOOGLE_CALENDAR_
            // CLIENT_ID`/`OAUTH_STATE_SECRET`, so no other test can observe
            // or race these process-global env writes.
            unsafe {
                std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", "true");
                std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", "test-client-id");
                std::env::set_var("OAUTH_STATE_SECRET", secret);
            }

            let token = oauth_state::sign_state(
                secret,
                account_id,
                Utc::now().timestamp(),
                "g41-org-gmail-still-works-nonce",
                Some(owner),
                Some("gmail"),
            )
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let router = Router::new()
                .route("/v1/oauth/{provider}/authorize", get(authorize))
                .with_state(app_state);
            let req = HttpRequest::builder()
                .uri(format!(
                    "/v1/oauth/google/authorize?surface=gmail&state={token}"
                ))
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();

            match prior_gmail_flag {
                Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
                None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
            }
            match prior_client_id {
                Some(v) => unsafe { std::env::set_var("GOOGLE_CALENDAR_CLIENT_ID", v) },
                None => unsafe { std::env::remove_var("GOOGLE_CALENDAR_CLIENT_ID") },
            }
            match prior_secret {
                Some(v) => unsafe { std::env::set_var("OAUTH_STATE_SECRET", v) },
                None => unsafe { std::env::remove_var("OAUTH_STATE_SECRET") },
            }

            assert_eq!(
                resp.status(),
                StatusCode::TEMPORARY_REDIRECT,
                "Gmail's own real org flow (its own flag on, callback's own Gmail arm allows \
                 it) must NOT be affected by the new pre-redirect check — regression coverage \
                 for the doomed-redirect fix above"
            );

            teardown(&base, db, pool).await;
        }

        // ── CS-431 (org-onboarding-from-the-invite.md §3 item 34): the
        // callback's `oauth_tokens` upsert must OVERWRITE a stored scope
        // string on re-consent, not merely insert-once — otherwise a
        // workspace that re-runs OAuth to pick up a newly required scope
        // would still read as stale afterward, and the connectors-page
        // re-consent prompt this story adds would never clear. Calls
        // [`store`] (which calls the REAL, unmodified `store_token_bundle_tx`
        // — the exact function [`callback`] uses) TWICE against the same
        // account/provider/workspace, simulating "installed before the
        // scope shipped" then "re-ran OAuth after it shipped" — not a
        // hand-rolled UPDATE, so this proves the production upsert's
        // `ON CONFLICT ... DO UPDATE SET scope = EXCLUDED.scope` arm, not a
        // copy of it. ────────────────────────────────────────────────────

        /// Slack's `TokenOwner::AccountWide` upsert target
        /// (`account_id, provider, provider_workspace_id) WHERE owner_user_id
        /// IS NULL`) — re-consenting the SAME team_id must UPDATE the
        /// existing row's `scope`, not insert a second one.
        #[tokio::test]
        async fn slack_reconnect_overwrites_stored_scope_pg() {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping slack_reconnect_overwrites_stored_scope_pg: set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "cs431_slack_reconnect_overwrite";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;

            // "Installed before `users:read.email` shipped" — a real, but
            // narrower, grant string, matching D17 point 5's exact shape.
            let stale_scope = "channels:history channels:read users:read reactions:read \
                                files:read canvases:read chat:write im:write channels:leave";
            store(
                &pool,
                account_id,
                "slack",
                TokenOwner::AccountWide,
                &slack_bundle("T-RECONSENT", Some(stale_scope)),
            )
            .await;

            let stored_before: String = sqlx::query_scalar(
                "SELECT scope FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(stored_before, stale_scope, "sanity: the stale row landed");
            assert_eq!(
                connectors::slack::slack_missing_scopes(&stored_before),
                Some(vec!["users:read.email".to_string()]),
                "sanity: the stale grant reads as missing users:read.email before reconnect"
            );

            // Re-run OAuth for the SAME team_id — the actual re-consent act.
            store(
                &pool,
                account_id,
                "slack",
                TokenOwner::AccountWide,
                &slack_bundle("T-RECONSENT", None), // None => today's real slack_scopes()
            )
            .await;

            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT scope FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                rows.len(),
                1,
                "re-consent must UPDATE the existing row, never insert a second one"
            );
            assert_eq!(
                rows[0].0,
                connectors::slack::slack_scopes(),
                "the stored scope must be the FRESH grant from the reconnect, not the stale one"
            );
            assert_eq!(
                connectors::slack::slack_missing_scopes(&rows[0].0),
                Some(Vec::new()),
                "after reconnect the re-consent prompt's own computation must find nothing missing"
            );

            teardown(&base, db, pool).await;
        }

        /// Google Chat's `TokenOwner::Member` upsert target
        /// (`account_id, owner_user_id, provider, provider_workspace_id)
        /// WHERE owner_user_id IS NOT NULL`) — same overwrite contract as
        /// Slack's above, on the OTHER partial unique index `store_token_
        /// bundle_tx` can hit.
        #[tokio::test]
        async fn google_chat_reconnect_overwrites_stored_scope_pg() {
            let Some(base) = base_url() else {
                eprintln!("skipping google_chat_reconnect_overwrites_stored_scope_pg: set ORGT_TEST_BASE_URL");
                return;
            };
            set_test_kek();
            let db = "cs431_chat_reconnect_overwrite";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member = seed_user(&pool).await;

            // "Connected before `directory.readonly` shipped" — the original
            // three-scope grant, matching D17 point 5's exact shape.
            let stale_scope = format!(
                "openid email {} {} {}",
                "https://www.googleapis.com/auth/chat.spaces.readonly",
                "https://www.googleapis.com/auth/chat.messages.readonly",
                "https://www.googleapis.com/auth/chat.memberships.readonly",
            );
            let mut stale_bundle = chat_bundle("cs431-member");
            stale_bundle.scope = stale_scope.clone();
            store(
                &pool,
                account_id,
                "google_chat",
                TokenOwner::Member(member),
                &stale_bundle,
            )
            .await;

            let stored: String = sqlx::query_scalar(
                "SELECT scope FROM oauth_tokens WHERE account_id = $1 AND provider = 'google_chat' \
                 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(member)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(stored, stale_scope, "sanity: the stale row landed");
            assert_eq!(
                connectors::google::chat_missing_scopes(&stored),
                Some(vec![
                    "https://www.googleapis.com/auth/directory.readonly".to_string()
                ]),
                "sanity: the stale grant reads as missing directory.readonly before reconnect"
            );

            // Re-run OAuth (Google's `prompt=consent&include_granted_scopes=
            // true`) for the SAME member/workspace — the actual re-consent
            // act. `chat_bundle` already carries today's real `data_scope()`.
            store(
                &pool,
                account_id,
                "google_chat",
                TokenOwner::Member(member),
                &chat_bundle("cs431-member"),
            )
            .await;

            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT scope FROM oauth_tokens WHERE account_id = $1 AND provider = 'google_chat' \
                 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(member)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                rows.len(),
                1,
                "re-consent must UPDATE the existing row, never insert a second one"
            );
            assert_eq!(
                rows[0].0,
                format!(
                    "openid email {}",
                    connectors::google::Surface::Chat.data_scope()
                ),
                "the stored scope must be the FRESH grant from the reconnect, not the stale one"
            );
            assert_eq!(
                connectors::google::chat_missing_scopes(&rows[0].0),
                Some(Vec::new()),
                "after reconnect the re-consent prompt's own computation must find nothing missing"
            );

            teardown(&base, db, pool).await;
        }

        /// CS-452, requirement (a): a member's disconnect of a per-member-
        /// scoped provider (Microsoft on the org plane) must NEVER touch
        /// this account's OWN `AccountWide` row for the SAME provider (a
        /// legacy Personal-plane connection an org later grew per-member
        /// rows alongside — see `resolve_delete_token_owner`'s own doc
        /// comment) or another member's own row. Direct proof of the exact
        /// defect that function fixes: the pre-CS-452 `owner_user_id IS
        /// NULL OR owner_user_id = $3` DELETE would have matched BOTH the
        /// `AccountWide` row and the caller's own `Member` row in ONE
        /// statement, since both share the SAME `(account_id, provider)` —
        /// wiping a connection every other member (and any AccountWide-only
        /// reader) still depends on.
        #[tokio::test]
        async fn delete_connection_member_owned_provider_leaves_the_accountwide_row_and_other_members_rows_intact_pg(
        ) {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping delete_connection_member_owned_provider_leaves_the_accountwide_row_and_other_members_rows_intact_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "cs452_member_leaves_accountwide_intact";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member_a = seed_user(&pool).await;
            let member_b = seed_user(&pool).await;

            // A legacy Personal-plane connection this account already had —
            // kept alive `AccountWide` alongside the org-plane per-member
            // rows below (exactly the coexistence
            // `resolve_delete_token_owner`'s own doc comment names).
            store(
                &pool,
                account_id,
                "microsoft",
                TokenOwner::AccountWide,
                &microsoft_bundle("legacy-accountwide"),
            )
            .await;
            store(
                &pool,
                account_id,
                "microsoft",
                TokenOwner::Member(member_a),
                &microsoft_bundle("member-a"),
            )
            .await;
            store(
                &pool,
                account_id,
                "microsoft",
                TokenOwner::Member(member_b),
                &microsoft_bundle("member-b"),
            )
            .await;

            // CodeRabbit review of #1052: `connector_installs` carries the
            // SAME `IS NULL OR = $n` fallback hazard the `oauth_tokens`
            // DELETE above already had — a shared `AccountWide` install row
            // for the SAME workspace id member_a's own token happens to use
            // (e.g. a legacy Personal-plane install, mirroring exactly the
            // `oauth_tokens` coexistence this test's own doc comment names).
            // `uq_connector_installs_account_wide`/`uq_connector_installs_
            // per_owner` (migration 0121) allow both to coexist for the same
            // `(provider, external_workspace_id, account_id)`.
            sqlx::query(
                "INSERT INTO connector_installs (provider, external_workspace_id, account_id, owner_user_id) \
                 VALUES ('microsoft', 'tenant-member-a', $1, NULL)",
            )
            .bind(account_id)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO connector_installs (provider, external_workspace_id, account_id, owner_user_id) \
                 VALUES ('microsoft', 'tenant-member-a', $1, $2)",
            )
            .bind(account_id)
            .bind(member_a)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO connector_installs (provider, external_workspace_id, account_id, owner_user_id) \
                 VALUES ('microsoft', 'tenant-member-b', $1, $2)",
            )
            .bind(account_id)
            .bind(member_b)
            .execute(&pool)
            .await
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let member_a_ctx = UserContext {
                user_id: member_a,
                account_id,
                role: "member".to_string(),
                device_id: "test-device".to_string(),
            };
            // Org tenant with NO real pool (`pool: None`) — the post-commit
            // Microsoft purge step degrades to its own `None` arm (logged,
            // skipped), which is fine: this test is scoped to the
            // `oauth_tokens` DELETE's owner-exactness. The purge step's own
            // owner-scoping is separately proven by `connectors::
            // microsoft_disconnect`'s `member_scoped_purge_touches_only_
            // that_members_rows_pg`, and the full org-plane flow (this
            // DELETE AND the tenant-plane purge together, with a REAL pool)
            // by this module's own
            // `delete_connection_microsoft_org_plane_full_flow_leaves_
            // member_bs_watches_and_subscriptions_intact_pg` below.
            let tenant = Tenant {
                store: None,
                pool: None,
                decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                    crate::tenancy::runtime::TenantContext {
                        org_id: "cs452_org_a".to_string(),
                        account_id,
                        user_id: member_a,
                        pool: sqlx::postgres::PgPoolOptions::new()
                            .connect_lazy("postgres://localhost/unused")
                            .expect("lazy pool construction never connects"),
                        page_pool: None,
                    },
                )),
                class: crate::route_class::RouteClass::Other,
            };

            let delete_resp = delete_connection(
                State(app_state.clone()),
                tenant,
                Extension(member_a_ctx),
                Path("microsoft".to_string()),
            )
            .await;
            assert_eq!(delete_resp.status(), StatusCode::OK);

            let remaining: Vec<(String, Option<Uuid>)> = sqlx::query_as(
                "SELECT provider, owner_user_id FROM oauth_tokens \
                 WHERE account_id = $1 ORDER BY owner_user_id NULLS FIRST",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                remaining.len(),
                2,
                "the AccountWide legacy row AND member_b's own row must survive member_a's \
                 disconnect: {remaining:?}"
            );
            assert!(
                remaining
                    .iter()
                    .any(|(p, o)| p == "microsoft" && o.is_none()),
                "the AccountWide row must survive — this is the exact defect CS-452 fixes: \
                 {remaining:?}"
            );
            assert!(
                remaining
                    .iter()
                    .any(|(p, o)| p == "microsoft" && *o == Some(member_b)),
                "member_b's own row must be untouched: {remaining:?}"
            );
            assert!(
                !remaining.iter().any(|(_, o)| *o == Some(member_a)),
                "member_a's own row must be gone: {remaining:?}"
            );

            let remaining_installs: Vec<(String, Option<Uuid>)> = sqlx::query_as(
                "SELECT external_workspace_id, owner_user_id FROM connector_installs \
                 WHERE account_id = $1 ORDER BY owner_user_id NULLS FIRST",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                remaining_installs.len(),
                2,
                "the AccountWide connector_installs row (workspace tenant-member-a) AND \
                 member_b's own (workspace tenant-member-b) must survive: {remaining_installs:?}"
            );
            assert!(
                remaining_installs
                    .iter()
                    .any(|(ws, o)| ws == "tenant-member-a" && o.is_none()),
                "the AccountWide connector_installs row sharing member_a's workspace id must \
                 survive — CodeRabbit review of #1052: {remaining_installs:?}"
            );
            assert!(
                remaining_installs
                    .iter()
                    .any(|(ws, o)| ws == "tenant-member-b" && *o == Some(member_b)),
                "member_b's own connector_installs row must be untouched: {remaining_installs:?}"
            );
            assert!(
                !remaining_installs.iter().any(|(_, o)| *o == Some(member_a)),
                "member_a's own connector_installs row must be gone: {remaining_installs:?}"
            );

            teardown(&base, db, pool).await;
        }

        /// CS-452, requirement (b): a FULL org-plane Microsoft disconnect
        /// through the REAL, unmodified `delete_connection` handler — with a
        /// REAL tenant pool, so its post-commit purge step actually runs —
        /// proving member_a disconnecting Microsoft leaves member_b's own
        /// `teams_watched_resources`/`graph_subscriptions` rows untouched.
        /// `connectors::microsoft_disconnect`'s own pg tests already prove
        /// `purge_microsoft_connection` itself is owner-correct in
        /// isolation; CS-452's actual production defect was one layer up —
        /// this route's caller passed `TokenOwner::AccountWide`
        /// UNCONDITIONALLY regardless of what `delete_connection` actually
        /// resolved for the `oauth_tokens` DELETE — so this test exercises
        /// the REAL call site, not just the purge function directly.
        ///
        /// Same "one physical Postgres server plays both the control AND
        /// tenant role" shape as this module's own
        /// `delete_connection_google_gmail_purges_the_watch_channel_through_the_real_capture_path_pg`
        /// above (`migrations_control`'s baseline already creates the
        /// shared tenant tables `purge_microsoft_connection` writes —
        /// `scripts/check_schema_parity.sh` is what keeps that fact true).
        #[tokio::test]
        async fn delete_connection_microsoft_org_plane_full_flow_leaves_member_bs_watches_and_subscriptions_intact_pg(
        ) {
            let Some(base) = base_url() else {
                eprintln!(
                    "skipping delete_connection_microsoft_org_plane_full_flow_leaves_member_bs_watches_and_subscriptions_intact_pg: \
                     set ORGT_TEST_BASE_URL"
                );
                return;
            };
            set_test_kek();
            let db = "cs452_microsoft_full_flow";
            let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
            let pool: PgPool = PgPoolOptions::new()
                .connect(&with_db(&base, db))
                .await
                .unwrap();
            sqlx::migrate!("./migrations_control")
                .run(&pool)
                .await
                .unwrap();

            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner).await;
            let member_a = seed_user(&pool).await;
            let member_b = seed_user(&pool).await;
            let graph_id: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary) \
                 VALUES ($1, 'test ocean', 'personal', true) RETURNING id",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();

            store(
                &pool,
                account_id,
                "microsoft",
                TokenOwner::Member(member_a),
                &microsoft_bundle("member-a"),
            )
            .await;
            store(
                &pool,
                account_id,
                "microsoft",
                TokenOwner::Member(member_b),
                &microsoft_bundle("member-b"),
            )
            .await;

            // member_a's own rows — must be purged by member_a's disconnect.
            sqlx::query(
                "INSERT INTO teams_watched_resources \
                     (account_id, graph_id, user_id, kind, team_id, channel_id, chat_id, tenant_id, auth_mode) \
                 VALUES ($1, $2, $3, 'channel', 'T1', 'C1', '', '', 'delegated')",
            )
            .bind(account_id)
            .bind(graph_id)
            .bind(member_a)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO graph_subscriptions \
                     (account_id, subscription_id, resource, client_state, expires_at, tenant_id) \
                 VALUES ($1, 'sub-a-channel', 'teams/T1/channels/C1/messages', 'cs', \
                         now() + interval '1 day', '')",
            )
            .bind(account_id)
            .execute(&pool)
            .await
            .unwrap();

            // member_b's own rows — must survive member_a's disconnect.
            sqlx::query(
                "INSERT INTO teams_watched_resources \
                     (account_id, graph_id, user_id, kind, team_id, channel_id, chat_id, tenant_id, auth_mode) \
                 VALUES ($1, $2, $3, 'chat', '', '', 'CHAT-B', 'tenant-xyz', 'app')",
            )
            .bind(account_id)
            .bind(graph_id)
            .bind(member_b)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO graph_subscriptions \
                     (account_id, subscription_id, resource, client_state, expires_at, tenant_id) \
                 VALUES ($1, 'sub-b-chat', 'chats/CHAT-B/messages', 'cs', \
                         now() + interval '1 day', 'tenant-xyz')",
            )
            .bind(account_id)
            .execute(&pool)
            .await
            .unwrap();

            let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            let member_a_ctx = UserContext {
                user_id: member_a,
                account_id,
                role: "member".to_string(),
                device_id: "test-device".to_string(),
            };
            // Org tenant WITH a real pool this time (`pool: Some(..)`), so
            // the post-commit Microsoft purge step actually runs — see this
            // test's own doc comment for why that distinction is exactly
            // what this test exists to cover.
            let tenant = Tenant {
                store: None,
                pool: Some(pool.clone()),
                decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                    crate::tenancy::runtime::TenantContext {
                        org_id: "cs452_org_b".to_string(),
                        account_id,
                        user_id: member_a,
                        pool: pool.clone(),
                        page_pool: None,
                    },
                )),
                class: crate::route_class::RouteClass::Other,
            };

            let delete_resp = delete_connection(
                State(app_state.clone()),
                tenant,
                Extension(member_a_ctx),
                Path("microsoft".to_string()),
            )
            .await;
            assert_eq!(delete_resp.status(), StatusCode::OK);

            let member_a_watch: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM teams_watched_resources WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account_id)
            .bind(member_a)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                member_a_watch, 0,
                "member_a's own watched Teams resource must be purged"
            );

            let member_b_watch: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM teams_watched_resources WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account_id)
            .bind(member_b)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                member_b_watch, 1,
                "member_b's own watched Teams resource must survive member_a's disconnect — this \
                 is the exact defect CS-452 fixes (the pre-CS-452 call site passed \
                 TokenOwner::AccountWide unconditionally, which would have purged it too)"
            );

            let sub_a_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM graph_subscriptions WHERE subscription_id = 'sub-a-channel')",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(!sub_a_exists, "member_a's own subscription must be gone");
            let sub_b_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM graph_subscriptions WHERE subscription_id = 'sub-b-chat')",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(
                sub_b_exists,
                "member_b's own subscription must survive untouched"
            );

            let remaining_tokens: Vec<Option<Uuid>> = sqlx::query_scalar(
                "SELECT owner_user_id FROM oauth_tokens WHERE account_id = $1 AND provider = 'microsoft'",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(
                remaining_tokens,
                vec![Some(member_b)],
                "only member_b's own oauth_tokens row must remain"
            );

            teardown(&base, db, pool).await;
        }
    }
}

// ── m365 enterprise train PR 3.2: admin-consent round trip (pg-gated) ────
//
// Against a REAL Postgres database: proves `record_microsoft_admin_consent`
// (the exact function `handle_microsoft_admin_consent` calls) round-trips
// a tenant's grant, that re-running admin-consent for the SAME tenant
// refreshes the row in place rather than duplicating it, and that two
// DIFFERENT tenants for the same account coexist as two rows. Same
// fixed-DB, fixed-KEK-not-needed (no encryption here), recreate-per-test
// conventions as `google_token_isolation_pg` above.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib routes::oauth::microsoft_admin_consent_pg_tests \
//     -- --test-threads=1
#[cfg(test)]
mod microsoft_admin_consent_pg_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn seed_account(pool: &PgPool, owner: Uuid) -> Uuid {
        // No `dek_wrapped` needed — `microsoft_admin_consents` stores no
        // token material, unlike `oauth_tokens`.
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn round_trips_account_tenant_consenting_user_and_scope_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping round_trips_account_tenant_consenting_user_and_scope_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_32_admin_consent_roundtrip";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let admin = seed_user(&pool).await;

        record_microsoft_admin_consent(
            &pool,
            account_id,
            "aaaabbbb-0000-cccc-1111-dddd2222eeee",
            Some(admin),
            Some(
                "https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/Calendars.Read",
            ),
        )
        .await
        .expect("record_microsoft_admin_consent");

        let row: (Uuid, String, Option<Uuid>, Option<String>) = sqlx::query_as(
            "SELECT account_id, microsoft_tenant_id, consented_by_user_id, granted_scope \
             FROM microsoft_admin_consents WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.0, account_id);
        assert_eq!(row.1, "aaaabbbb-0000-cccc-1111-dddd2222eeee");
        assert_eq!(row.2, Some(admin));
        assert_eq!(
            row.3.as_deref(),
            Some(
                "https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/Calendars.Read"
            )
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn reconsenting_the_same_tenant_updates_in_place_not_a_duplicate_row_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping reconsenting_the_same_tenant_updates_in_place_not_a_duplicate_row_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_32_admin_consent_reconsent";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let first_admin = seed_user(&pool).await;
        let second_admin = seed_user(&pool).await;

        record_microsoft_admin_consent(
            &pool,
            account_id,
            "tenant-1111",
            Some(first_admin),
            Some("Mail.Read"),
        )
        .await
        .expect("first consent");

        // A different admin re-runs admin-consent for the SAME tenant
        // later (e.g. after a scope-set change) — must refresh the
        // existing row, not create a second one.
        record_microsoft_admin_consent(
            &pool,
            account_id,
            "tenant-1111",
            Some(second_admin),
            Some("Mail.Read Calendars.Read"),
        )
        .await
        .expect("second consent (re-run)");

        let rows: Vec<(String, Option<Uuid>, Option<String>)> = sqlx::query_as(
            "SELECT microsoft_tenant_id, consented_by_user_id, granted_scope \
             FROM microsoft_admin_consents WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(
            rows.len(),
            1,
            "re-consenting the same tenant must not duplicate: {rows:?}"
        );
        assert_eq!(rows[0].1, Some(second_admin));
        assert_eq!(rows[0].2.as_deref(), Some("Mail.Read Calendars.Read"));

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn two_different_tenants_for_the_same_account_coexist_as_two_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping two_different_tenants_for_the_same_account_coexist_as_two_rows_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_32_admin_consent_two_tenants";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;

        record_microsoft_admin_consent(&pool, account_id, "tenant-a", None, None)
            .await
            .expect("tenant a");
        record_microsoft_admin_consent(&pool, account_id, "tenant-b", None, None)
            .await
            .expect("tenant b");

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM microsoft_admin_consents WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 2);

        teardown(&base, db, pool).await;
    }
}

// ---------------------------------------------------------------------------
// `resolve_callback_dek_plane`'s decision table (pg-gated)
// ---------------------------------------------------------------------------
//
// The OAuth callback is an unauthenticated browser redirect: the binding gate
// never sees it, so it has no `TenancyDecision` and must resolve the account's
// DEK plane itself. Getting that decision wrong is what produced the 503 of
// 2026-09-06 (`{"error":"encryption key not configured"}` on every org
// connector install), and getting it wrong the OTHER way — resolving an org
// with a live tenant database onto the consumer plane — would be cross-tenant
// key confusion. So the whole table is asserted here against real databases,
// including a real `TenantRegistry` build over a real second Postgres.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib routes::oauth::callback_dek_plane_pg_tests \
//     -- --test-threads=1
#[cfg(test)]
mod callback_dek_plane_pg_tests {
    use super::*;
    use crate::tenancy::registry::TenantRegistry;
    use crate::tenancy::runtime::TenantRuntime;
    use crate::tenancy::secrets::StaticMapResolver;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgPool};

    const ORG_ID: &str = "org_callback_plane";
    const APEX: &str = "coralswarm.test";
    const SECRET_REF: &str = "TENANT_DB_CALLBACK_PLANE";

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    fn set_test_kek() {
        // SAFETY: every pg test in this process writes the SAME constant.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }
    async fn recreate(base: &str, db: &str) {
        let mut admin = sqlx::PgConnection::connect(base)
            .await
            .expect("connect admin");
        admin
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
    }
    async fn drop_dbs(base: &str, dbs: [&str; 2], pools: [PgPool; 2]) {
        for p in pools {
            p.close().await;
        }
        let mut admin = sqlx::PgConnection::connect(base).await.unwrap();
        for db in dbs {
            admin
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    struct Fixture {
        state: Arc<AppState>,
        control: PgPool,
        tenant: PgPool,
        org_account: Uuid,
        personal_account: Uuid,
        org_dek: [u8; 32],
        decoy_control_dek: [u8; 32],
        personal_dek: [u8; 32],
    }

    /// Control DB + tenant DB + an enabled `TenantRuntime` whose registry
    /// resolves `SECRET_REF` to the tenant DB. The org account carries a
    /// DECOY `accounts.dek_wrapped` so a cross-plane fallback cannot pass by
    /// accident (it would resolve *a* key), and its REAL key lives in the
    /// tenant DB's `account_keys`. `tenant_status` seeds the routing row;
    /// `None` seeds no routing row at all.
    async fn fixture(base: &str, cdb: &str, tdb: &str, tenant_status: Option<&str>) -> Fixture {
        set_test_kek();
        let kek = load_kek().expect("test KEK loads");
        recreate(base, cdb).await;
        recreate(base, tdb).await;

        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(base, cdb))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();
        let tenant: PgPool = PgPoolOptions::new()
            .connect(&with_db(base, tdb))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&tenant)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(ORG_ID)
            .execute(&tenant)
            .await
            .unwrap();

        let mk_user = |email: &'static str| {
            let control = control.clone();
            async move {
                sqlx::query_scalar::<_, Uuid>(
                    "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
                )
                .bind(format!("{}-{}@x.co", email, Uuid::new_v4()))
                .fetch_one(&control)
                .await
                .unwrap()
            }
        };

        let decoy_control_dek = generate_dek();
        let org_owner = mk_user("org").await;
        let org_account: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(org_owner)
        .bind(ORG_ID)
        .bind(wrap_dek(&kek, &decoy_control_dek).unwrap())
        .fetch_one(&control)
        .await
        .unwrap();

        let personal_dek = generate_dek();
        let personal_owner = mk_user("personal").await;
        let personal_account: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
        )
        .bind(personal_owner)
        .bind(wrap_dek(&kek, &personal_dek).unwrap())
        .fetch_one(&control)
        .await
        .unwrap();

        let org_dek = generate_dek();
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(org_account)
            .bind(wrap_dek(&kek, &org_dek).unwrap())
            .execute(&tenant)
            .await
            .unwrap();

        if let Some(status) = tenant_status {
            // Enter the real account/period financial transaction for this guarded fixture write.
            let mut fixture_financial_tx = crate::tenancy::scope::begin_account_scoped(&control, org_account)
                .await.unwrap();
            crate::billing::renewal::lock_period_rotation(&mut fixture_financial_tx, org_account)
                .await.unwrap();
            sqlx::query(
                "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
                 VALUES ($1, $2, 'cbplane', $3, $4)",
            )
            .bind(ORG_ID)
            .bind(org_account)
            .bind(SECRET_REF)
            .bind(status)
            .execute(&mut *fixture_financial_tx)
            .await
            .unwrap();
            fixture_financial_tx.commit().await.unwrap();
        }

        let resolver = StaticMapResolver::from_pairs([(SECRET_REF, with_db(base, tdb))]);
        let mut state = crate::test_app_state_owned(Some(TenantRuntime::new(
            true,
            APEX,
            Arc::new(TenantRegistry::new(Arc::new(resolver))),
        )));
        state.db = Some(control.clone());

        Fixture {
            state: Arc::new(state),
            control,
            tenant,
            org_account,
            personal_account,
            org_dek,
            decoy_control_dek,
            personal_dek,
        }
    }

    /// An `active` tenant: an org account resolves to its OWN tenant plane and
    /// the key it yields is the tenant `account_keys` one, never the decoy
    /// sitting in control's `accounts.dek_wrapped` for the same account. A
    /// personal account on the same runtime still resolves to control.
    #[tokio::test]
    async fn active_tenant_resolves_the_org_to_its_own_tenant_plane_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping active_tenant_resolves_the_org_to_its_own_tenant_plane_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let f = fixture(
            &base,
            "orgt_cbplane_active_ctl",
            "orgt_cbplane_active_ten",
            Some("active"),
        )
        .await;

        let plane = resolve_callback_dek_plane(&f.state, &f.control, f.org_account, Some(ORG_ID))
            .await
            .expect("an active tenant must resolve, not refuse");
        assert!(
            plane.is_org(),
            "an onboarded org must resolve to its tenant plane"
        );
        assert_eq!(
            plane.resolve(f.org_account).await.unwrap().unwrap(),
            f.org_dek,
            "must be the TENANT account_keys DEK, not control's decoy"
        );
        assert_ne!(f.org_dek, f.decoy_control_dek);

        let personal = resolve_callback_dek_plane(&f.state, &f.control, f.personal_account, None)
            .await
            .expect("a personal account always resolves");
        assert!(!personal.is_org());
        assert_eq!(
            personal.resolve(f.personal_account).await.unwrap().unwrap(),
            f.personal_dek
        );

        drop_dbs(
            &base,
            ["orgt_cbplane_active_ctl", "orgt_cbplane_active_ten"],
            [f.control, f.tenant],
        )
        .await;
    }

    /// `provisioning` is writable (onboarding provisions the tenant DEK at
    /// step 8, before the routing row flips `active` at step 9 — same gate
    /// `routes::admin::rotate_dek` uses), while `suspended`/`offboarded`
    /// REFUSE rather than guess a plane.
    #[tokio::test]
    async fn provisioning_is_writable_and_frozen_statuses_refuse_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping provisioning_is_writable_and_frozen_statuses_refuse_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let f = fixture(
            &base,
            "orgt_cbplane_status_ctl",
            "orgt_cbplane_status_ten",
            Some("provisioning"),
        )
        .await;

        let plane = resolve_callback_dek_plane(&f.state, &f.control, f.org_account, Some(ORG_ID))
            .await
            .expect("provisioning must be writable — onboarding step 8 depends on it");
        assert!(plane.is_org());
        assert_eq!(
            plane.resolve(f.org_account).await.unwrap().unwrap(),
            f.org_dek
        );

        // Only the two real write-freeze states are exercised here: a
        // genuinely unrecognized status (the `None` arm, which this handler
        // also refuses) cannot be INSERTED at all —
        // `tenant_databases_status_valid` is a DB CHECK constraint, verified
        // by this test's own first draft failing with 23514 on
        // `'bogus_status'`. That arm stays as defence in depth for a future
        // status value the constraint gains before this match does.
        for frozen in ["suspended", "offboarded"] {
            sqlx::query("UPDATE tenant_databases SET status = $1 WHERE clerk_org_id = $2")
                .bind(frozen)
                .bind(ORG_ID)
                .execute(&f.control)
                .await
                .unwrap();
            let refused =
                resolve_callback_dek_plane(&f.state, &f.control, f.org_account, Some(ORG_ID)).await;
            let resp = refused
                .err()
                .unwrap_or_else(|| panic!("status {frozen} must refuse, never resolve a plane"));
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        }

        drop_dbs(
            &base,
            ["orgt_cbplane_status_ctl", "orgt_cbplane_status_ten"],
            [f.control, f.tenant],
        )
        .await;
    }

    /// The routing row's `account_id` is cross-checked against the account the
    /// SIGNED STATE bound — the same assert `routes::admin::rotate_dek` makes
    /// before writing a DEK into a tenant's `account_keys`. A disagreement is
    /// a refusal, never a write against the other account's plane.
    #[tokio::test]
    async fn routing_row_account_mismatch_refuses_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping routing_row_account_mismatch_refuses_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let f = fixture(
            &base,
            "orgt_cbplane_mismatch_ctl",
            "orgt_cbplane_mismatch_ten",
            Some("active"),
        )
        .await;

        // `personal_account` is a real account, but it is NOT the account the
        // routing row names for this org.
        let refused =
            resolve_callback_dek_plane(&f.state, &f.control, f.personal_account, Some(ORG_ID))
                .await;
        let resp = refused
            .err()
            .expect("an account that does not own this tenant must be refused");
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        drop_dbs(
            &base,
            ["orgt_cbplane_mismatch_ctl", "orgt_cbplane_mismatch_ten"],
            [f.control, f.tenant],
        )
        .await;
    }

    /// An org that was never onboarded onto a tenant database (no
    /// `tenant_databases` row) keeps its DEK on control — that is where
    /// `clerk_provision::ensure_org_account` put it, and prod has exactly one
    /// such org. Same answer when the tenancy runtime is disabled entirely.
    /// Neither is a "fallback": in both cases control is the org's only plane.
    #[tokio::test]
    async fn never_onboarded_org_and_flag_off_both_resolve_to_control_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping never_onboarded_org_and_flag_off_both_resolve_to_control_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let f = fixture(
            &base,
            "orgt_cbplane_norow_ctl",
            "orgt_cbplane_norow_ten",
            None,
        )
        .await;

        let plane = resolve_callback_dek_plane(&f.state, &f.control, f.org_account, Some(ORG_ID))
            .await
            .expect("no routing row ⇒ control, not a refusal");
        assert!(!plane.is_org());
        assert_eq!(
            plane.resolve(f.org_account).await.unwrap().unwrap(),
            f.decoy_control_dek,
            "with no tenant database, control's own key IS this org's key"
        );

        // Runtime disabled ⇒ same answer, even with a live routing row.
        // Enter the real account/period financial transaction for this guarded fixture write.
        let mut fixture_financial_tx = crate::tenancy::scope::begin_account_scoped(&f.control, f.org_account)
            .await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut fixture_financial_tx, f.org_account)
            .await.unwrap();
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ($1, $2, 'cbplaneoff', $3, 'active')",
        )
        .bind(ORG_ID)
        .bind(f.org_account)
        .bind(SECRET_REF)
        .execute(&mut *fixture_financial_tx)
        .await
        .unwrap();
        fixture_financial_tx.commit().await.unwrap();
        let mut off = (*f.state).clone();
        off.tenant_runtime = Some(TenantRuntime::new(
            false,
            APEX,
            Arc::new(TenantRegistry::new(Arc::new(
                StaticMapResolver::from_pairs([(SECRET_REF, "postgres://unused/unused")]),
            ))),
        ));
        let off_plane =
            resolve_callback_dek_plane(&Arc::new(off), &f.control, f.org_account, Some(ORG_ID))
                .await
                .expect("MULTITENANT_ENABLED off ⇒ control is the only plane");
        assert!(!off_plane.is_org());

        drop_dbs(
            &base,
            ["orgt_cbplane_norow_ctl", "orgt_cbplane_norow_ten"],
            [f.control, f.tenant],
        )
        .await;
    }
}

// ---------------------------------------------------------------------------
// Slack member (user-scope-only) connect — CS-454, item 46a (pg-gated)
// ---------------------------------------------------------------------------
//
// Proves, against a REAL Postgres database, the claims this story's PR
// description makes: a non-admin org member can complete the `slack_member`
// callback (the admin gate is genuinely skipped, not merely untested); the
// resulting row is `oauth_tokens.provider = 'slack_user'`,
// `TokenOwner::Member(uid)`; that row can never satisfy the account-wide
// bot-token lookup shape (`workers::huddle::resolve_account_wide_oauth_token`
// hardcodes `provider = 'slack'` AND `owner_user_id IS NULL` — mirrored here
// rather than called directly, since that function is private to a different
// module, same "re-execute the real shape, cross-referenced by file:function"
// discipline `connector_installs_pg.rs`'s own header documents for itself);
// a wrong-workspace `authed_user` is rejected before any row is written; and
// disconnect removes only the disconnecting member's own rows, never the
// shared bot row or another member's.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib routes::oauth::slack_member_connect_pg_tests \
//     -- --test-threads=1
#[cfg(test)]
mod slack_member_connect_pg_tests {
    use super::*;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Serializes every test in this module that mutates the process-global
    /// `OAUTH_STATE_SECRET` / `ORG_SLACK_OAUTH_ENABLED` / `SLACK_API_BASE` /
    /// `SLACK_CLIENT_ID` / `SLACK_CLIENT_SECRET` env vars — same shape as
    /// `routes::oauth::tests::OAUTH_STATE_SECRET_ENV_LOCK` and
    /// `connectors::slack::SLACK_API_BASE_ENV_LOCK`, but local to THIS
    /// module: neither of those is reachable from here (both are private to
    /// their own defining module), and this module's own CI step already
    /// runs it in total isolation (a single named `--lib` filter under
    /// `--test-threads=1`) — this lock is defense in depth for an
    /// unfiltered local `cargo test --lib` run, not load-bearing in CI.
    pub(super) static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn set_test_kek() {
        // SAFETY: single-threaded within this module's own lock — see
        // `ENV_LOCK`'s own doc comment.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// An account with a real wrapped DEK — every `store_token_bundle_tx`
    /// call needs one. `clerk_org_id` is `Some(..)` for the org-plane tests
    /// (the admin gate only exists under an org account) and `None` for the
    /// pure DB-shape tests that don't care about the plane.
    async fn seed_account_with_dek(pool: &PgPool, owner: Uuid, clerk_org_id: Option<&str>) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped, clerk_org_id) VALUES ($1, $2, $3) \
             RETURNING id",
        )
        .bind(owner)
        .bind(&wrapped)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// `seat_tier_id` (PR F, seat lifecycle): `account_members_default_seat_tier`
    /// rejects any insert that omits it — every writer must pass one
    /// explicitly, same discipline `clerk_provision.rs`'s own test fixtures
    /// already follow (`seed_team_org_with_member`'s own `tier_id` lookup).
    async fn set_account_member_role(pool: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
        let tier_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(pool)
        .await
        .expect("seat_tiers 'standard' must be seeded (0165)");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, $3, $4)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(role)
        .bind(tier_id)
        .execute(pool)
        .await
        .unwrap();
    }

    fn bot_bundle(workspace_id: &str) -> TokenBundle {
        TokenBundle {
            access_token: format!("xoxb-bot-{workspace_id}"),
            refresh_token: None,
            user_token: None,
            scope: slack_scopes_for_test(),
            expires_at: None,
            provider_workspace_id: workspace_id.to_string(),
            provider_workspace_name: Some(format!("Workspace {workspace_id}")),
            provider_user_id: format!("U-ADMIN-{workspace_id}"),
        }
    }
    fn slack_scopes_for_test() -> String {
        "channels:history channels:read users:read users:read.email chat:write".to_string()
    }

    /// Same shape as [`callback`]'s own generic write path — used to seed a
    /// existing shared and member installs for isolation assertions without
    /// going through the HTTP callback for every fixture.
    async fn store_with_install(
        pool: &PgPool,
        account_id: Uuid,
        provider: &str,
        connector_installs_provider: &str,
        owner: TokenOwner,
        bundle: &TokenBundle,
    ) {
        store_with_install_on_plane(
            pool,
            &crate::crypto::DekPlane::consumer(pool.clone()),
            account_id,
            provider,
            connector_installs_provider,
            owner,
            bundle,
        )
        .await
    }

    async fn store_with_install_on_plane(
        pool: &PgPool,
        dek_plane: &crate::crypto::DekPlane,
        account_id: Uuid,
        provider: &str,
        connector_installs_provider: &str,
        owner: TokenOwner,
        bundle: &TokenBundle,
    ) {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .expect("begin scoped tx");
        store_token_bundle_tx(&mut tx, dek_plane, account_id, provider, owner, bundle)
            .await
            .expect("store_token_bundle_tx");
        record_connector_install(
            &mut tx,
            account_id,
            connector_installs_provider,
            &bundle.provider_workspace_id,
            Some(bundle.provider_user_id.as_str()),
            owner.owner_user_id(),
        )
        .await
        .expect("record_connector_install");
        tx.commit().await.expect("commit");
    }

    /// Seed the legacy `slack_integrations` mirror a bot install's
    /// `post_store` would have written — member-connect tests that assert
    /// the bot token is unchanged need this row to exist first.
    async fn seed_slack_integrations(
        pool: &PgPool,
        account_id: Uuid,
        workspace_id: &str,
        bot_token: &str,
    ) {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .expect("begin scoped tx");
        sqlx::query(
            "INSERT INTO slack_integrations \
             (account_id, workspace_id, workspace_name, bot_token, created_at) \
             VALUES ($1, $2, $3, $4, now())",
        )
        .bind(account_id)
        .bind(workspace_id)
        .bind(format!("Workspace {workspace_id}"))
        .bind(bot_token)
        .execute(&mut *tx)
        .await
        .expect("seed slack_integrations");
        tx.commit().await.expect("commit slack_integrations");
    }

    /// A tiny in-process mock Slack server: `/oauth.v2.access` answers a
    /// MEMBER-only exchange (no top-level bot fields — see
    /// `connectors::slack::exchange_code_member`'s own doc comment for why
    /// that shape matters) for `team_id`, and `/auth.revoke` answers `ok`
    /// and records whether it was called. Same pattern
    /// `connectors::history_backfill`'s own pg tests already use for
    /// mocking Slack HTTP (`axum::serve` on an ephemeral `127.0.0.1:0`
    /// listener) — see that module's own test for the precedent.
    async fn spawn_mock_slack_member_server(
        team_id: &'static str,
        authed_user_id: &'static str,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        use axum::{routing::post, Json, Router};
        let revoke_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let revoke_called_clone = revoke_called.clone();

        async fn oauth_v2_access(
            axum::extract::State((team_id, authed_user_id)): axum::extract::State<(
                &'static str,
                &'static str,
            )>,
        ) -> Json<serde_json::Value> {
            Json(serde_json::json!({
                "ok": true,
                "team": { "id": team_id, "name": format!("Workspace {team_id}") },
                "authed_user": {
                    "id": authed_user_id,
                    "scope": "channels:history channels:read groups:history groups:read \
                              im:history im:read mpim:history mpim:read users:read \
                              users:read.email",
                    "access_token": format!("xoxp-member-{authed_user_id}"),
                    "token_type": "user"
                }
            }))
        }
        async fn auth_revoke(
            axum::extract::State(flag): axum::extract::State<
                std::sync::Arc<std::sync::atomic::AtomicBool>,
            >,
        ) -> Json<serde_json::Value> {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Json(serde_json::json!({ "ok": true, "revoked": true }))
        }

        let oauth_app = Router::new()
            .route("/oauth.v2.access", post(oauth_v2_access))
            .with_state((team_id, authed_user_id));
        let revoke_app = Router::new()
            .route("/auth.revoke", post(auth_revoke))
            .with_state(revoke_called_clone);
        let app = oauth_app.merge(revoke_app);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock slack");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (addr, revoke_called)
    }

    fn callback_params(code: &str, state: &str) -> CallbackParams {
        CallbackParams {
            code: Some(code.to_string()),
            state: Some(state.to_string()),
            error: None,
            admin_consent: None,
            tenant: None,
            scope: None,
            installation_id: None,
            setup_action: None,
        }
    }

    // ── resolve_slack_token_target: pure logic, no DB ──────────────────────

    #[test]
    fn resolve_slack_token_target_pure_logic() {
        let uid = Uuid::new_v4();

        // No surface claim at all (every state minted before this story, and
        // every plain admin bot-install connect after it) — byte-identical
        // pre-existing shape.
        assert_eq!(
            resolve_slack_token_target(None, None).unwrap(),
            (TokenOwner::AccountWide, "slack")
        );
        assert_eq!(
            resolve_slack_token_target(None, Some(uid)).unwrap(),
            (TokenOwner::AccountWide, "slack"),
            "a connecting user id with no slack_member claim still resolves AccountWide"
        );

        // A DIFFERENT surface claim (defense in depth — never reachable from
        // a real Slack state, but must not be confused with slack_member).
        assert_eq!(
            resolve_slack_token_target(Some("gmail"), Some(uid)).unwrap(),
            (TokenOwner::AccountWide, "slack")
        );

        // The member surface, with a connecting user id.
        assert_eq!(
            resolve_slack_token_target(Some(SLACK_MEMBER_SURFACE), Some(uid)).unwrap(),
            (TokenOwner::Member(uid), "slack_user")
        );

        // The member surface with NO connecting user id — must refuse, never
        // silently fall back to AccountWide (the no-fallback rule every
        // sibling resolver in this file documents for itself).
        assert!(resolve_slack_token_target(Some(SLACK_MEMBER_SURFACE), None).is_err());

        // The install-and-sign-in surface (connectors redesign, B2): the primary
        // row is the workspace bot, exactly as a plain install; the member's own
        // row is written beside it. It needs a signed caller, with no fallback.
        assert_eq!(
            resolve_slack_token_target(Some(SLACK_INSTALL_SURFACE), Some(uid)).unwrap(),
            (TokenOwner::AccountWide, "slack")
        );
        assert!(resolve_slack_token_target(Some(SLACK_INSTALL_SURFACE), None).is_err());
        assert!(is_slack_install_surface("slack", Some(SLACK_INSTALL_SURFACE)));
        assert!(!is_slack_install_surface("slack", Some(SLACK_MEMBER_SURFACE)));
        assert!(!is_slack_install_surface("google", Some(SLACK_INSTALL_SURFACE)));
    }

    // ── End-to-end: a non-admin member completes the callback ──────────────

    #[tokio::test]
    async fn slack_member_connect_admits_non_admin_and_isolates_the_row_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_member_connect_admits_non_admin_and_isolates_the_row_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "cs454_member_connect_non_admin";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner, Some("org_cs454_admits")).await;
        set_account_member_role(&pool, account_id, owner, "owner").await;
        let member = seed_user(&pool).await;
        // The plainest possible non-admin role — this is the entire point of
        // item 46a's "lift the admin gate".
        set_account_member_role(&pool, account_id, member, "member").await;

        store_with_install(
            &pool,
            account_id,
            "slack",
            "slack",
            TokenOwner::AccountWide,
            &bot_bundle("T-REAL"),
        )
        .await;
        seed_slack_integrations(&pool, account_id, "T-REAL", "xoxb-bot-T-REAL").await;

        let (addr, _revoke_called) = spawn_mock_slack_member_server("T-REAL", "U-MEMBER-1").await;

        // SAFETY: held `_guard` (ENV_LOCK) above serializes every test in
        // this module that mutates these process-global env vars.
        unsafe {
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "cs454-test-secret-do-not-use-in-prod-0001",
            );
            std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
            std::env::set_var("SLACK_CLIENT_ID", "test-client-id");
            std::env::set_var("SLACK_CLIENT_SECRET", "test-client-secret");
            std::env::set_var("SLACK_API_BASE", format!("http://{addr}"));
        }

        let secret = oauth_state::state_secret().unwrap();
        let nonce = Uuid::new_v4().to_string();
        let now = Utc::now().timestamp();
        let state_token = oauth_state::sign_state(
            &secret,
            account_id,
            now,
            &nonce,
            Some(member),
            Some(SLACK_MEMBER_SURFACE),
        )
        .unwrap();

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let resp = callback(
            State(app_state.clone()),
            Path("slack".to_string()),
            Query(callback_params("test-code-member-connect", &state_token)),
        )
        .await;

        assert_ne!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "a non-admin member's slack_member connect must NOT be refused by the admin gate \
             (item 46a's own point) — got {:?}",
            resp.status()
        );
        assert!(
            resp.status().is_redirection(),
            "a successful callback redirects to /connectors?connected=slack — got {:?}",
            resp.status()
        );

        // ── The row is provider `slack_user`, TokenOwner::Member(uid) ──────
        let row: (String, Option<Uuid>, String, String) = sqlx::query_as(
            "SELECT provider, owner_user_id, scope, provider_workspace_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack_user'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("the slack_user row must exist");
        assert_eq!(row.0, "slack_user");
        assert_eq!(
            row.1,
            Some(member),
            "owner_user_id must be the connecting member, not NULL"
        );
        assert!(
            row.2.contains("users:read.email"),
            "the granted user scope must be stored verbatim: {}",
            row.2
        );
        assert_eq!(row.3, "T-REAL");

        // ── connector_installs gained a per-owner row under provider
        // 'slack' (never 'slack_user') — routing still works for member
        // connects (item 3). ─────────────────────────────────────────────
        let install_owner: Option<Uuid> = sqlx::query_scalar(
            "SELECT owner_user_id FROM connector_installs \
             WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id = 'T-REAL' \
               AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&pool)
        .await
        .expect("a per-owner connector_installs row must exist under provider 'slack'");
        assert_eq!(install_owner, Some(member));

        // ── It never satisfies the account-wide bot-token lookup ───────────
        // Mirrors `workers::huddle::resolve_account_wide_oauth_token`'s own
        // WHERE clause exactly (provider = 'slack' AND owner_user_id IS
        // NULL) — that function is private to a different module, so this
        // re-executes its literal shape, cross-referenced here by
        // file:function, same discipline `connector_installs_pg.rs`'s own
        // header documents for itself.
        let bot_lookup: Option<String> = sqlx::query_scalar(
            "SELECT access_token::text FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack' AND provider_workspace_id = 'T-REAL' \
               AND owner_user_id IS NULL \
             LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert!(
            bot_lookup.is_some(),
            "the account-wide bot row must still resolve on its own"
        );
        let still_only_one_accountwide_row: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack' AND owner_user_id IS NULL",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            still_only_one_accountwide_row, 1,
            "the member connect must not have touched or duplicated the account-wide bot row"
        );

        // ── connector_installs dedupes correctly (migration 0252) ──────────
        let resolved: Vec<Uuid> =
            sqlx::query_scalar("SELECT resolve_connector_install_account_ids($1, $2)")
                .bind("slack")
                .bind("T-REAL")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            resolved,
            vec![account_id],
            "the account must resolve EXACTLY ONCE despite holding both an account-wide row and \
             a per-owner row for this workspace — a duplicate would double-process every inbound \
             Slack event for this account (see migration 0252's own header)"
        );

        // CS-454 CodeRabbit: a member callback must not overwrite the
        // workspace bot token via `SlackConnector::post_store`.
        let bot_token_after: String = sqlx::query_scalar(
            "SELECT bot_token FROM slack_integrations \
             WHERE account_id = $1 AND workspace_id = 'T-REAL'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("the pre-existing slack_integrations row must still exist");
        assert_eq!(
            bot_token_after, "xoxb-bot-T-REAL",
            "member connect must not replace slack_integrations.bot_token with the xoxp- user token"
        );

        teardown(&base, db, pool).await;
    }

    // ── Install-and-sign-in (connectors redesign, B2, decision 4) ───────────

    /// A mock Slack for ONE consent that returns both tokens: the workspace bot
    /// and the member's own, as `oauth.v2.access` does when the authorize URL
    /// asked for `scope` and `user_scope` together.
    async fn spawn_mock_slack_install_server(
        team_id: &'static str,
        authed_user_id: &'static str,
    ) -> std::net::SocketAddr {
        use axum::{routing::post, Json, Router};
        async fn oauth_v2_access(
            axum::extract::State((team_id, authed_user_id)): axum::extract::State<(
                &'static str,
                &'static str,
            )>,
        ) -> Json<serde_json::Value> {
            Json(serde_json::json!({
                "ok": true,
                "access_token": format!("xoxb-bot-{team_id}"),
                "token_type": "bot",
                "scope": crate::connectors::slack::slack_scopes().replace(' ', ","),
                "team": { "id": team_id, "name": format!("Workspace {team_id}") },
                "authed_user": {
                    "id": authed_user_id,
                    "scope": "channels:history,channels:read,users:read,users:read.email",
                    "access_token": format!("xoxp-member-{authed_user_id}"),
                    "token_type": "user"
                }
            }))
        }
        let app = Router::new()
            .route("/oauth.v2.access", post(oauth_v2_access))
            .with_state((team_id, authed_user_id));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock slack");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        addr
    }

    fn location_of(resp: &Response) -> String {
        resp.headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    }

    #[tokio::test]
    async fn slack_install_connect_stores_the_bot_and_the_members_own_token_from_one_consent_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_install_connect_stores_the_bot_and_the_members_own_token_from_one_consent_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "b2_slack_install_connect";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        // A personal account: its owner is the member who connects.
        let member = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, member, None).await;
        let addr = spawn_mock_slack_install_server("T-INSTALL", "U-INSTALLER").await;

        // SAFETY: `_guard` (ENV_LOCK) serializes every test here that writes these.
        unsafe {
            std::env::set_var("OAUTH_STATE_SECRET", "b2-install-test-secret-do-not-use-in-prod-01");
            std::env::set_var("SLACK_CLIENT_ID", "test-client-id");
            std::env::set_var("SLACK_CLIENT_SECRET", "test-client-secret");
        }
        // Restores `SLACK_API_BASE` when the test ends, and keeps the other Slack
        // suites' mocks from seeing this one's address.
        let _slack_lock = crate::connectors::slack::SLACK_API_BASE_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let _slack_base =
            crate::connectors::slack::SlackApiBaseTestGuard::set(&format!("http://{addr}"));
        let secret = oauth_state::state_secret().unwrap();
        let state_token = oauth_state::sign_state_for_connect(
            &secret,
            account_id,
            Utc::now().timestamp(),
            &Uuid::new_v4().to_string(),
            Some(member),
            Some(SLACK_INSTALL_SURFACE),
            "wizard",
        )
        .unwrap();

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let web = app_state.personal_web_base_url();
        let resp = callback(
            State(app_state.clone()),
            Path("slack".to_string()),
            Query(callback_params("install-code", &state_token)),
        )
        .await;

        // The browser lands on the return page, not the older /connectors.
        assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            location_of(&resp),
            format!("{web}/connectors/return?provider=slack&result=ok")
        );

        // The workspace bot: account-wide, and WITHOUT the member's token on it.
        let bot: (Option<Uuid>, bool) = sqlx::query_as(
            "SELECT owner_user_id, user_token_encrypted IS NOT NULL FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("the bot row must exist");
        assert_eq!(bot, (None, false), "the bot row is the workspace's, with no member token");

        // The member's own row, from the same consent, with the scope Slack granted.
        let me: (Option<Uuid>, String, String) = sqlx::query_as(
            "SELECT owner_user_id, scope, provider_workspace_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack_user'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("the member's slack_user row must exist");
        assert_eq!(me.0, Some(member));
        assert!(me.1.contains("users:read.email"), "{}", me.1);
        assert_eq!(me.2, "T-INSTALL");

        // Both route through `provider = 'slack'`, once each.
        let installs: Vec<Option<Uuid>> = sqlx::query_scalar(
            "SELECT owner_user_id FROM connector_installs \
             WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id = 'T-INSTALL' \
             ORDER BY owner_user_id NULLS FIRST",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(installs, vec![None, Some(member)]);

        // The connector reads as just connected: one source row, off, not set up.
        let rows: Vec<(String, bool, bool)> = sqlx::query_as(
            "SELECT source, enabled, setup_completed_at IS NOT NULL FROM connector_sources \
             WHERE account_id = $1 AND owner_user_id = $2 AND provider = 'slack'",
        )
        .bind(account_id)
        .bind(member)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows, vec![("channels".to_string(), false, false)]);

        // The state is single use, even for a state that carries `ret`.
        let replay = callback(
            State(app_state.clone()),
            Path("slack".to_string()),
            Query(callback_params("install-code", &state_token)),
        )
        .await;
        assert_eq!(
            location_of(&replay),
            format!("{web}/connectors/return?provider=slack&result=error&code=invalid_request")
        );

        teardown(&base, db, pool).await;
    }

    /// An org with a plain `member` who connects through the connector API, a
    /// mock Slack for `team`, and the env a callback needs. Returns the pool, the
    /// account, the member, the member's signed state and the mock's base URL.
    async fn org_install_fixture(
        base: &str,
        db: &str,
        org: &str,
        team: &'static str,
    ) -> (PgPool, Uuid, Uuid, String, String) {
        let conn = recreate(base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner, Some(org)).await;
        set_account_member_role(&pool, account_id, owner, "owner").await;
        let member = seed_user(&pool).await;
        set_account_member_role(&pool, account_id, member, "member").await;
        let addr = spawn_mock_slack_install_server(team, "U-MEMBER-2").await;
        // SAFETY: the caller holds `ENV_LOCK`, which serializes every test here
        // that writes these.
        unsafe {
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "b2-install-test-secret-do-not-use-in-prod-01",
            );
            std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
            std::env::set_var("SLACK_CLIENT_ID", "test-client-id");
            std::env::set_var("SLACK_CLIENT_SECRET", "test-client-secret");
        }
        let secret = oauth_state::state_secret().unwrap();
        let state_token = oauth_state::sign_state_for_connect(
            &secret,
            account_id,
            Utc::now().timestamp(),
            &Uuid::new_v4().to_string(),
            Some(member),
            Some(SLACK_INSTALL_SURFACE),
            "wizard",
        )
        .unwrap();
        (pool, account_id, member, state_token, format!("http://{addr}"))
    }

    #[tokio::test]
    async fn slack_install_connect_under_an_org_is_open_to_any_member_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_install_connect_under_an_org_is_open_to_any_member_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "b2_slack_install_org";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (pool, account_id, member, state_token, slack_addr) =
            org_install_fixture(&base, db, "org_b2_install", "T-ORG").await;
        let _slack_lock = crate::connectors::slack::SLACK_API_BASE_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let _slack_base = crate::connectors::slack::SlackApiBaseTestGuard::set(&slack_addr);

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let web = app_state.personal_web_base_url();
        let resp = callback(
            State(app_state),
            Path("slack".to_string()),
            Query(callback_params("install-code", &state_token)),
        )
        .await;

        // Decision 2026-10-29: a plain org member installs the app as part of
        // their own connect.
        assert_eq!(
            location_of(&resp),
            format!("{web}/connectors/return?provider=slack&result=ok")
        );
        let providers: Vec<(String, Option<Uuid>)> = sqlx::query_as(
            "SELECT provider, owner_user_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider LIKE 'slack%' ORDER BY provider",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            providers,
            vec![("slack".to_string(), None), ("slack_user".to_string(), Some(member))]
        );

        // Both credential rows are audited, attributed to the member.
        let audit: Vec<(String, Option<Uuid>, String)> = sqlx::query_as(
            "SELECT provider, actor_user_id, actor_kind FROM connector_audit_log \
             WHERE account_id = $1 AND action = 'install' ORDER BY provider",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(audit.len(), 2, "{audit:?}");
        assert!(audit.iter().all(|(_, actor, _)| *actor == Some(member)), "{audit:?}");
        assert_eq!(audit[0].0, "slack");
        assert_eq!(audit[1].0, "slack_user");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn a_member_install_leaves_a_healthy_bot_row_alone_and_a_dead_one_is_replaced_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_member_install_leaves_a_healthy_bot_row_alone_and_a_dead_one_is_replaced_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "b2_slack_install_keep_bot";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (pool, account_id, member, state_token, slack_addr) =
            org_install_fixture(&base, db, "org_b2_keep", "T-KEEP").await;
        let _slack_lock = crate::connectors::slack::SLACK_API_BASE_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let _slack_base = crate::connectors::slack::SlackApiBaseTestGuard::set(&slack_addr);

        // An admin installed the app earlier: healthy, with every scope.
        let mut admin_bot = bot_bundle("T-KEEP");
        admin_bot.scope = crate::connectors::slack::slack_scopes();
        store_with_install(
            &pool,
            account_id,
            "slack",
            "slack",
            TokenOwner::AccountWide,
            &admin_bot,
        )
        .await;
        seed_slack_integrations(&pool, account_id, "T-KEEP", "xoxb-bot-T-KEEP").await;
        let bot_before: (Vec<u8>, DateTime<Utc>) = sqlx::query_as(
            "SELECT access_token, updated_at FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let web = app_state.personal_web_base_url();
        let resp = callback(
            State(app_state.clone()),
            Path("slack".to_string()),
            Query(callback_params("install-code", &state_token)),
        )
        .await;
        assert_eq!(
            location_of(&resp),
            format!("{web}/connectors/return?provider=slack&result=ok")
        );

        // The admin's bot row and the ingest mirror are exactly as they were.
        let bot_after: (Vec<u8>, DateTime<Utc>) = sqlx::query_as(
            "SELECT access_token, updated_at FROM oauth_tokens \
             WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(bot_after, bot_before, "a healthy bot row must not be rewritten");
        let mirror: String = sqlx::query_scalar(
            "SELECT bot_token FROM slack_integrations WHERE account_id = $1 AND workspace_id = 'T-KEEP'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(mirror, "xoxb-bot-T-KEEP");

        // The member is signed in, routed, and audited; no bot install is claimed.
        let owners: Vec<Option<Uuid>> = sqlx::query_scalar(
            "SELECT owner_user_id FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack_user'",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(owners, vec![Some(member)]);
        let audit: Vec<(String, Option<Uuid>)> = sqlx::query_as(
            "SELECT provider, actor_user_id FROM connector_audit_log \
             WHERE account_id = $1 AND action = 'install'",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(audit, vec![("slack_user".to_string(), Some(member))]);

        // A dead bot is replaced by a member's install: that is how it heals.
        sqlx::query(
            "UPDATE oauth_tokens SET credential_invalid_at = now(), credential_invalid_reason = 'x' \
             WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .unwrap();
        let secret = oauth_state::state_secret().unwrap();
        let second = oauth_state::sign_state_for_connect(
            &secret,
            account_id,
            Utc::now().timestamp(),
            &Uuid::new_v4().to_string(),
            Some(member),
            Some(SLACK_INSTALL_SURFACE),
            "wizard",
        )
        .unwrap();
        let resp = callback(
            State(app_state),
            Path("slack".to_string()),
            Query(callback_params("install-code", &second)),
        )
        .await;
        assert_eq!(
            location_of(&resp),
            format!("{web}/connectors/return?provider=slack&result=ok")
        );
        let healed: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT credential_invalid_at FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(healed.is_none(), "a reinstall must replace a dead bot row");

        teardown(&base, db, pool).await;
    }

    // ── Wrong-workspace authed_user is rejected ─────────────────────────────

    #[tokio::test]
    async fn slack_member_connect_rejects_wrong_workspace_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_member_connect_rejects_wrong_workspace_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "cs454_member_connect_wrong_ws";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner, Some("org_cs454_wrongws")).await;
        let member = seed_user(&pool).await;
        set_account_member_role(&pool, account_id, member, "member").await;

        // The account's bot install is for T-REAL...
        store_with_install(
            &pool,
            account_id,
            "slack",
            "slack",
            TokenOwner::AccountWide,
            &bot_bundle("T-REAL"),
        )
        .await;
        // ...but the mock Slack server answers this exchange for a
        // DIFFERENT workspace, T-OTHER — simulating an authed_user whose
        // team does not match any workspace this account has installed.
        let (addr, _revoke_called) = spawn_mock_slack_member_server("T-OTHER", "U-MEMBER-2").await;

        unsafe {
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "cs454-test-secret-do-not-use-in-prod-0002",
            );
            std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
            std::env::set_var("SLACK_CLIENT_ID", "test-client-id");
            std::env::set_var("SLACK_CLIENT_SECRET", "test-client-secret");
            std::env::set_var("SLACK_API_BASE", format!("http://{addr}"));
        }

        let secret = oauth_state::state_secret().unwrap();
        let nonce = Uuid::new_v4().to_string();
        let now = Utc::now().timestamp();
        let state_token = oauth_state::sign_state(
            &secret,
            account_id,
            now,
            &nonce,
            Some(member),
            Some(SLACK_MEMBER_SURFACE),
        )
        .unwrap();

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let resp = callback(
            State(app_state.clone()),
            Path("slack".to_string()),
            Query(callback_params("test-code-wrong-ws", &state_token)),
        )
        .await;

        assert_eq!(
            resp.status(),
            StatusCode::CONFLICT,
            "a member's authorized team that does not match any workspace this account has \
             installed must be refused — got {:?}",
            resp.status()
        );

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack_user'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            count, 0,
            "no slack_user row may be written for a rejected wrong-workspace connect"
        );

        teardown(&base, db, pool).await;
    }

    async fn complete_member_callback(
        app_state: &Arc<AppState>,
        account_id: Uuid,
        member: Option<Uuid>,
    ) -> Response {
        let state_token = oauth_state::sign_state(
            &oauth_state::state_secret().unwrap(),
            account_id,
            Utc::now().timestamp(),
            &Uuid::new_v4().to_string(),
            member,
            Some(SLACK_MEMBER_SURFACE),
        )
        .unwrap();
        callback(
            State(app_state.clone()),
            Path("slack".to_string()),
            Query(callback_params("test-code-member", &state_token)),
        )
        .await
    }

    fn configure_member_oauth(addr: std::net::SocketAddr) {
        // SAFETY: callers hold ENV_LOCK and run this suite in isolation.
        unsafe {
            std::env::set_var(
                "OAUTH_STATE_SECRET",
                "single-slack-test-secret-do-not-use-in-prod",
            );
            std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
            std::env::set_var("SLACK_CLIENT_ID", "test-client-id");
            std::env::set_var("SLACK_CLIENT_SECRET", "test-client-secret");
            std::env::set_var("SLACK_API_BASE", format!("http://{addr}"));
        }
    }

    // First personal owner, org owner, and non-admin can each connect
    // without a bot. A second member gets a separate, owner-exact grant.
    #[tokio::test]
    async fn slack_member_connect_without_bot_is_owner_exact_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping slack_member_connect_without_bot_is_owner_exact_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "cs454_member_connect_no_bot";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let pool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        let (addr, _) = spawn_mock_slack_member_server("T-FIRST", "U-FIRST").await;
        configure_member_oauth(addr);
        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        for (org, role) in [
            (None, "owner"),
            (Some("org_first_owner"), "owner"),
            (Some("org_first_member"), "member"),
        ] {
            let owner = seed_user(&pool).await;
            let account_id = seed_account_with_dek(&pool, owner, org).await;
            let member = if role == "owner" {
                owner
            } else {
                seed_user(&pool).await
            };
            if org.is_some() {
                set_account_member_role(&pool, account_id, member, role).await;
            }
            let response = complete_member_callback(&app_state, account_id, Some(member)).await;
            assert!(
                response.status().is_redirection(),
                "first {role} connect without a bot must succeed: {:?}",
                response.status()
            );

            let rows: Vec<(String, Option<Uuid>, Option<String>)> = sqlx::query_as(
                "SELECT provider, owner_user_id, provider_workspace_id FROM oauth_tokens WHERE account_id=$1",
            ).bind(account_id).fetch_all(&pool).await.unwrap();
            assert_eq!(
                rows,
                vec![(
                    "slack_user".to_string(),
                    Some(member),
                    Some("T-FIRST".to_string())
                )]
            );
            let installs: Vec<Option<Uuid>> = sqlx::query_scalar(
                "SELECT owner_user_id FROM connector_installs WHERE account_id=$1 AND provider='slack' AND external_workspace_id='T-FIRST'",
            ).bind(account_id).fetch_all(&pool).await.unwrap();
            assert_eq!(
                installs,
                vec![Some(member)],
                "only the connecting person's routing row is created"
            );
            let mirrors: i64 =
                sqlx::query_scalar("SELECT count(*) FROM slack_integrations WHERE account_id=$1")
                    .bind(account_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(mirrors, 0, "user tokens must never populate the bot mirror");

            if role == "member" {
                let first: (Vec<u8>, DateTime<Utc>) = sqlx::query_as(
                    "SELECT access_token, updated_at FROM oauth_tokens WHERE account_id=$1 AND provider='slack_user' AND owner_user_id=$2",
                ).bind(account_id).bind(member).fetch_one(&pool).await.unwrap();
                let second = seed_user(&pool).await;
                set_account_member_role(&pool, account_id, second, "member").await;
                let (second_addr, _) = spawn_mock_slack_member_server("T-FIRST", "U-SECOND").await;
                configure_member_oauth(second_addr);
                assert!(
                    complete_member_callback(&app_state, account_id, Some(second))
                        .await
                        .status()
                        .is_redirection()
                );
                let owners: Vec<Uuid> = sqlx::query_scalar(
                    "SELECT owner_user_id FROM oauth_tokens WHERE account_id=$1 AND provider='slack_user' ORDER BY owner_user_id",
                ).bind(account_id).fetch_all(&pool).await.unwrap();
                let mut expected = vec![member, second];
                expected.sort();
                assert_eq!(
                    owners, expected,
                    "second connect must preserve independent grants"
                );
                let first_after: (Vec<u8>, DateTime<Utc>) = sqlx::query_as(
                    "SELECT access_token, updated_at FROM oauth_tokens WHERE account_id=$1 AND provider='slack_user' AND owner_user_id=$2",
                ).bind(account_id).bind(member).fetch_one(&pool).await.unwrap();
                assert_eq!(
                    first_after, first,
                    "second connect must not rewrite the first grant"
                );
                let install_owners: Vec<Uuid> = sqlx::query_scalar(
                    "SELECT owner_user_id FROM connector_installs WHERE account_id=$1 AND provider='slack' ORDER BY owner_user_id",
                ).bind(account_id).fetch_all(&pool).await.unwrap();
                assert_eq!(install_owners, expected);
                let resolved: Vec<Uuid> = sqlx::query_scalar(
                    "SELECT resolve_connector_install_account_ids('slack', 'T-FIRST')",
                )
                .fetch_all(&pool)
                .await
                .unwrap();
                assert_eq!(
                    resolved.iter().filter(|&&id| id == account_id).count(),
                    1,
                    "member-only routing must resolve once"
                );
            }
        }
        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn slack_member_connect_rejects_ineligible_signed_caller_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping slack_member_connect_rejects_ineligible_signed_caller_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "cs454_member_connect_ineligible";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let pool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        let (addr, _) = spawn_mock_slack_member_server("T-FIRST", "U-FIRST").await;
        configure_member_oauth(addr);
        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let owner = seed_user(&pool).await;
        let outsider = seed_user(&pool).await;
        let org_account = seed_account_with_dek(&pool, owner, Some("org_ineligible_slack")).await;
        let personal_account = seed_account_with_dek(&pool, owner, None).await;
        for account in [org_account, personal_account] {
            assert_eq!(
                complete_member_callback(&app_state, account, Some(outsider))
                    .await
                    .status(),
                StatusCode::FORBIDDEN
            );
            assert_eq!(
                complete_member_callback(&app_state, account, None)
                    .await
                    .status(),
                StatusCode::BAD_REQUEST
            );
        }
        set_account_member_role(&pool, org_account, outsider, "member").await;
        // A state minted while the caller was eligible cannot survive removal.
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
            .bind(org_account)
            .bind(outsider)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            complete_member_callback(&app_state, org_account, Some(outsider))
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        unsafe { std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "false") };
        set_account_member_role(&pool, org_account, owner, "owner").await;
        assert_eq!(
            complete_member_callback(&app_state, org_account, Some(owner))
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        configure_member_oauth(addr);
        sqlx::query("UPDATE users SET deleted_at=now() WHERE id=$1")
            .bind(owner)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            complete_member_callback(&app_state, personal_account, Some(owner))
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        let tokens: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens")
            .fetch_one(&pool)
            .await
            .unwrap();
        let installs: i64 = sqlx::query_scalar("SELECT count(*) FROM connector_installs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            (tokens, installs),
            (0, 0),
            "rejected callbacks must persist no grants or routing rows"
        );
        teardown(&base, db, pool).await;
    }

    // ── Disconnect removes only the disconnecting member's own rows ────────

    #[tokio::test]
    async fn slack_member_disconnect_removes_only_that_members_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_member_disconnect_removes_only_that_members_rows_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "cs454_member_disconnect";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner, Some("org_cs454_disc")).await;
        let member1 = seed_user(&pool).await;
        let member2 = seed_user(&pool).await;

        // The shared bot row.
        store_with_install(
            &pool,
            account_id,
            "slack",
            "slack",
            TokenOwner::AccountWide,
            &bot_bundle("T-REAL"),
        )
        .await;
        // Two members' own rows for the same workspace.
        let mut member1_bundle = bot_bundle("T-REAL");
        member1_bundle.access_token = "xoxp-member1".to_string();
        member1_bundle.provider_user_id = "U-MEMBER-1".to_string();
        store_with_install(
            &pool,
            account_id,
            "slack_user",
            "slack",
            TokenOwner::Member(member1),
            &member1_bundle,
        )
        .await;
        let mut member2_bundle = bot_bundle("T-REAL");
        member2_bundle.access_token = "xoxp-member2".to_string();
        member2_bundle.provider_user_id = "U-MEMBER-2".to_string();
        store_with_install(
            &pool,
            account_id,
            "slack_user",
            "slack",
            TokenOwner::Member(member2),
            &member2_bundle,
        )
        .await;

        for member in [member1, member2] {
            set_account_member_role(&pool, account_id, member, "member").await;
            crate::connectors::slack_member_capture::sync_member_channels(
                &pool, account_id, member, "T-REAL", &["C-SHARED".to_string()],
            ).await.unwrap();
        }

        let (addr, revoke_called) = spawn_mock_slack_member_server("T-REAL", "U-MEMBER-1").await;
        unsafe {
            std::env::set_var("SLACK_API_BASE", format!("http://{addr}"));
        }

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let owner_ctx = UserContext {
            user_id: owner,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };
        assert_eq!(
            delete_slack_member_connection(&app_state, &Tenant::legacy(&app_state), &owner_ctx)
                .await
                .status(),
            StatusCode::NOT_FOUND,
            "org owner without their own grant cannot disconnect another person's grant",
        );
        assert!(!revoke_called.load(std::sync::atomic::Ordering::SeqCst));
        let member1_ctx = UserContext {
            user_id: member1,
            account_id,
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };
        let resp =
            delete_slack_member_connection(&app_state, &Tenant::legacy(&app_state), &member1_ctx)
                .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "member1's own disconnect must succeed"
        );

        assert!(
            revoke_called.load(std::sync::atomic::Ordering::SeqCst),
            "auth.revoke must have been called for member1's token before it was deleted"
        );

        // member1's own row is gone, both tables.
        let member1_tokens: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack_user' \
             AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(member1)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(member1_tokens, 0, "member1's oauth_tokens row must be gone");
        let member1_installs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND provider = 'slack' \
             AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(member1)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            member1_installs, 0,
            "member1's connector_installs row must be gone"
        );

        let index_owners: Vec<Uuid> = sqlx::query_scalar(
            "SELECT user_id FROM slack_member_channels WHERE account_id=$1",
        ).bind(account_id).fetch_all(&pool).await.unwrap();
        assert_eq!(index_owners, vec![member2], "only the disconnected caller's index is cleared");
        // Simulate a refresh that finished after disconnect: its stale index
        // must not make the caller eligible to capture again.
        crate::connectors::slack_member_capture::sync_member_channels(
            &pool, account_id, member1, "T-REAL", &["C-SHARED".to_string()],
        ).await.unwrap();
        let targets = crate::connectors::slack_member_capture::member_capture_targets(
            &pool, "T-REAL", "C-SHARED",
        ).await.unwrap();
        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].user_id, member2);
        let bodies = crate::connectors::slack_member_capture::member_bodies(
            &pool, "T-REAL", "C-SHARED", "1.0", "event body",
            |_| async { crate::connectors::slack_member_capture::MemberToken::NoToken },
            |_| async { crate::connectors::slack_member_capture::MemberFetch::NotAttempted },
        ).await.unwrap();
        assert!(bodies.is_empty(), "a token disappearing after target lookup must never capture event text");

        // member2's row survives, untouched.
        let member2_tokens: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack_user' \
             AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(member2)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            member2_tokens, 1,
            "member2's own row must survive member1's disconnect"
        );
        let member2_installs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND provider = 'slack' \
             AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(member2)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            member2_installs, 1,
            "member2's connector_installs row must survive"
        );

        // The shared bot row survives, untouched.
        let bot_tokens: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack' \
             AND owner_user_id IS NULL",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            bot_tokens, 1,
            "the shared bot row must survive a member's own disconnect"
        );
        let bot_installs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND provider = 'slack' \
             AND owner_user_id IS NULL",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            bot_installs, 1,
            "the shared bot connector_installs row must survive"
        );

        // Disconnecting again (already gone) is a clean 404, not a crash.
        let resp_again =
            delete_slack_member_connection(&app_state, &Tenant::legacy(&app_state), &member1_ctx)
                .await;
        assert_eq!(resp_again.status(), StatusCode::NOT_FOUND);

        teardown(&base, db, pool).await;
    }

    // ── Admin bot disconnect also clears member Slack rows ──────────────────

    #[tokio::test]
    async fn slack_admin_disconnect_clears_member_rows_for_that_workspace_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_admin_disconnect_clears_member_rows_for_that_workspace_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "cs454_admin_disconnect_cascade";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner, Some("org_cs454_admin_disc")).await;
        set_account_member_role(&pool, account_id, owner, "owner").await;
        let member = seed_user(&pool).await;
        set_account_member_role(&pool, account_id, member, "member").await;

        store_with_install(
            &pool,
            account_id,
            "slack",
            "slack",
            TokenOwner::AccountWide,
            &bot_bundle("T-REAL"),
        )
        .await;
        seed_slack_integrations(&pool, account_id, "T-REAL", "xoxb-bot-T-REAL").await;

        let mut member_bundle = bot_bundle("T-REAL");
        member_bundle.access_token = "xoxp-member-real".to_string();
        member_bundle.provider_user_id = "U-MEMBER-REAL".to_string();
        store_with_install(
            &pool,
            account_id,
            "slack_user",
            "slack",
            TokenOwner::Member(member),
            &member_bundle,
        )
        .await;

        // Unrelated workspace: must survive a T-REAL bot disconnect.
        let mut other_bundle = bot_bundle("T-OTHER");
        other_bundle.access_token = "xoxp-member-other".to_string();
        other_bundle.provider_user_id = "U-MEMBER-OTHER".to_string();
        store_with_install(
            &pool,
            account_id,
            "slack_user",
            "slack",
            TokenOwner::Member(member),
            &other_bundle,
        )
        .await;

        // Unrelated provider: must survive.
        let gmail = TokenBundle {
            access_token: "ya29-gmail".to_string(),
            refresh_token: None,
            user_token: None,
            scope: "https://www.googleapis.com/auth/gmail.readonly".to_string(),
            expires_at: None,
            provider_workspace_id: "member@example.com".to_string(),
            provider_workspace_name: None,
            provider_user_id: "gmail-sub".to_string(),
        };
        store_with_install(
            &pool,
            account_id,
            "google_gmail",
            "google_gmail",
            TokenOwner::Member(member),
            &gmail,
        )
        .await;

        let (addr, revoke_called) = spawn_mock_slack_member_server("T-REAL", "U-MEMBER-REAL").await;
        unsafe {
            std::env::set_var("SLACK_API_BASE", format!("http://{addr}"));
        }

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let owner_ctx = UserContext {
            user_id: owner,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };
        // `pool: None` skips the tenant-plane purge (not under test here);
        // `decision: None` is the personal/flag-off plane the existing
        // `delete_connection` tests use.
        let tenant = Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        };
        let resp = delete_connection(
            State(app_state.clone()),
            tenant,
            Extension(owner_ctx),
            Path("slack".to_string()),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "admin bot disconnect must succeed — got {:?}",
            resp.status()
        );

        assert!(
            revoke_called.load(std::sync::atomic::Ordering::SeqCst),
            "admin bot disconnect must auth.revoke the member token for the disconnected workspace"
        );

        let real_member_tokens: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack_user' \
             AND provider_workspace_id = 'T-REAL'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            real_member_tokens, 0,
            "slack_user rows for the disconnected workspace must be gone"
        );
        let real_member_installs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND provider = 'slack' \
             AND external_workspace_id = 'T-REAL' AND owner_user_id IS NOT NULL",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            real_member_installs, 0,
            "per-owner slack connector_installs for the disconnected workspace must be gone"
        );

        let other_member_tokens: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack_user' \
             AND provider_workspace_id = 'T-OTHER'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            other_member_tokens, 1,
            "a slack_user row for a different workspace must survive"
        );
        let gmail_tokens: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'google_gmail'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(gmail_tokens, 1, "unrelated provider rows must survive");

        assert!(
            find_other_org_slack_workspace(&pool, account_id, "T-NEW")
                .await
                .unwrap()
                .is_some(),
            "the surviving T-OTHER member row is a different workspace — reconnect to T-NEW \
             is still blocked by THAT workspace, not by leftover T-REAL member rows"
        );
        assert!(
            find_other_org_slack_workspace(&pool, account_id, "T-OTHER")
                .await
                .unwrap()
                .is_none(),
            "T-REAL member/bot rows must be gone, so T-OTHER is the only remaining Slack workspace"
        );

        teardown(&base, db, pool).await;
    }

    // ── Member disconnect decrypts on the tenant DEK plane ─────────────────

    #[tokio::test]
    async fn slack_member_disconnect_revokes_with_tenant_dek_not_control_decoy_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_member_disconnect_revokes_with_tenant_dek_not_control_decoy_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let kek = load_kek().expect("test KEK loads");
        let cdb = "cs454_member_disc_org_dek_ctl";
        let tdb = "cs454_member_disc_org_dek_ten";
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, cdb);
        let conn = recreate(&base, cdb).await;
        conn.close().await.ok();
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, tdb);
        let conn = recreate(&base, tdb).await;
        conn.close().await.ok();

        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, cdb))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();
        let tenant_pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, tdb))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&tenant_pool)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let decoy_control_dek = generate_dek();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped, clerk_org_id) VALUES ($1, $2, $3) \
             RETURNING id",
        )
        .bind(owner)
        .bind(wrap_dek(&kek, &decoy_control_dek).expect("wrap decoy"))
        .bind("org_cs454_dek")
        .fetch_one(&control)
        .await
        .unwrap();
        let member = seed_user(&control).await;
        set_account_member_role(&control, account_id, member, "member").await;

        let org_dek = generate_dek();
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(account_id)
            .bind(wrap_dek(&kek, &org_dek).expect("wrap tenant dek"))
            .execute(&tenant_pool)
            .await
            .unwrap();
        assert_ne!(org_dek, decoy_control_dek);

        let mut member_bundle = bot_bundle("T-REAL");
        member_bundle.access_token = "xoxp-member-org-dek".to_string();
        member_bundle.provider_user_id = "U-MEMBER-ORG".to_string();
        let tenant_plane = crate::crypto::DekPlane::org(
            "org_cs454_dek".to_string(),
            tenant_pool.clone(),
            account_id,
        );
        store_with_install_on_plane(
            &control,
            &tenant_plane,
            account_id,
            "slack_user",
            "slack",
            TokenOwner::Member(member),
            &member_bundle,
        )
        .await;

        let (addr, revoke_called) = spawn_mock_slack_member_server("T-REAL", "U-MEMBER-ORG").await;
        unsafe {
            std::env::set_var("SLACK_API_BASE", format!("http://{addr}"));
        }

        let app_state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));
        let member_ctx = UserContext {
            user_id: member,
            account_id,
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };
        let tenant = Tenant {
            store: None,
            pool: Some(tenant_pool.clone()),
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_cs454_dek".to_string(),
                    account_id,
                    user_id: member,
                    pool: tenant_pool.clone(),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        };

        let resp = delete_slack_member_connection(&app_state, &tenant, &member_ctx).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "org member disconnect must succeed — got {:?}",
            resp.status()
        );
        assert!(
            revoke_called.load(std::sync::atomic::Ordering::SeqCst),
            "auth.revoke must run: decrypt has to use the tenant account_keys DEK, not the \
             decoy sitting in control accounts.dek_wrapped"
        );

        control.close().await;
        tenant_pool.close().await;
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin
            .execute(format!("DROP DATABASE IF EXISTS {cdb} WITH (FORCE)").as_str())
            .await
            .ok();
        admin
            .execute(format!("DROP DATABASE IF EXISTS {tdb} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // A first-run checklist asks "has THIS member connected anything with
    // their own grant" (D21). The listing returns the shared account-wide
    // rows too, so without `mine` a member would read the bot row another
    // member installed as their own connection.
    #[tokio::test]
    async fn list_connections_marks_only_the_callers_own_rows_as_mine_pg() {
        let Some(base) = base_url() else {
            assert!(
                std::env::var("REQUIRE_PG_SUITE").is_err(),
                "REQUIRE_PG_SUITE set without ORGT_TEST_BASE_URL"
            );
            eprintln!(
                "skipping list_connections_marks_only_the_callers_own_rows_as_mine_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _guard = ENV_LOCK.lock().await;
        set_test_kek();
        let db = "selfserve_d_list_connections_mine";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let org_id = format!("org_selfserve_d_{}", Uuid::new_v4().simple());
        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner, Some(&org_id)).await;
        set_account_member_role(&pool, account_id, owner, "owner").await;
        let me = seed_user(&pool).await;
        set_account_member_role(&pool, account_id, me, "member").await;
        let other = seed_user(&pool).await;
        set_account_member_role(&pool, account_id, other, "member").await;

        store_with_install(
            &pool,
            account_id,
            "slack",
            "slack",
            TokenOwner::AccountWide,
            &bot_bundle("T-REAL"),
        )
        .await;
        let mut mine_bundle = bot_bundle("T-REAL");
        mine_bundle.access_token = "xoxp-me".to_string();
        mine_bundle.provider_user_id = "U-ME".to_string();
        store_with_install(
            &pool,
            account_id,
            "slack_user",
            "slack",
            TokenOwner::Member(me),
            &mine_bundle,
        )
        .await;
        let mut other_bundle = bot_bundle("T-OTHER");
        other_bundle.access_token = "xoxp-other".to_string();
        other_bundle.provider_user_id = "U-OTHER".to_string();
        store_with_install(
            &pool,
            account_id,
            "slack_user",
            "slack",
            TokenOwner::Member(other),
            &other_bundle,
        )
        .await;

        // Restores the prior value on drop, so a failed assertion below does
        // not leave the flag on for the next test. Declared after `_guard`,
        // so it drops (and restores) while ENV_LOCK is still held.
        struct SlackFlagOn(Option<String>);
        impl Drop for SlackFlagOn {
            fn drop(&mut self) {
                // SAFETY: ENV_LOCK is still held; see the declaration order.
                unsafe {
                    match &self.0 {
                        Some(v) => std::env::set_var("ORG_SLACK_OAUTH_ENABLED", v),
                        None => std::env::remove_var("ORG_SLACK_OAUTH_ENABLED"),
                    }
                }
            }
        }
        let _slack_flag = SlackFlagOn(std::env::var("ORG_SLACK_OAUTH_ENABLED").ok());
        // SAFETY: held `_guard` (ENV_LOCK) serializes every env mutation in
        // this module.
        unsafe {
            std::env::set_var("ORG_SLACK_OAUTH_ENABLED", "true");
        }

        let app_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let me_ctx = UserContext {
            user_id: me,
            account_id,
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };
        let tenant = Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: org_id.clone(),
                    account_id,
                    user_id: me,
                    pool: pool.clone(),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        };
        let resp = list_connections(State(app_state), tenant, Extension(me_ctx)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let rows: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let rows = rows.as_array().expect("connections array");

        let by_provider = |p: &str| -> Vec<&serde_json::Value> {
            rows.iter().filter(|r| r["provider"] == p).collect()
        };
        let bot = by_provider("slack");
        assert_eq!(bot.len(), 1, "the shared bot row is listed: {rows:?}");
        assert_eq!(
            bot[0]["mine"],
            json!(false),
            "an account-wide row is never the caller's own connection: {rows:?}"
        );
        let member_rows = by_provider("slack_user");
        assert_eq!(
            member_rows.len(),
            1,
            "only the caller's own member row is listed, never another member's: {rows:?}"
        );
        assert_eq!(member_rows[0]["provider_workspace_id"], json!("T-REAL"));
        assert_eq!(
            member_rows[0]["mine"],
            json!(true),
            "the caller's own member row is marked mine: {rows:?}"
        );

        drop(_slack_flag);
        teardown(&base, db, pool).await;
    }
}
