//! Google Chat watched-space model (gsuite Train G4 PR 3 — `.scratch/
//! gsuite-enterprise/g4-chat-design.md` §3.1/§3.2/§5.2, migration
//! `0198_google_chat_watched_spaces` / tenant `0073`).
//!
//! This module owns the **container** (`spaceType == SPACE`) archetype's
//! lifecycle: the app-install gate (D6, §3.1), the per-space destination
//! picker (§3.2), and the Chat API calls
//! (`spaces.get`/`spaces.list`/`spaces.members.list`) both need. Message
//! normalisation lives in [`super::chat_body`] (G4.2); the actual
//! message-ingest funnel (backfill/catch-up/live push) is G4.4's.
//!
//! # D6 — the row's EXISTENCE is the authorising act, not its destination
//!
//! `ADDED_TO_SPACE` ([`record_added_to_space`], called from
//! `routes::google_push`'s new Chat-app-interaction-event arm) creates a
//! `google_chat_watched_spaces` row in an **unrouted** state (`mode IS
//! NULL`) the moment CoralSwarm's Chat app joins a space — independent of
//! whether any human has since picked a destination. That row's existence
//! is contract §4's C2 = present-and-VISIBLE signal (Google's own "X added
//! CoralSwarm to this space" system message is what makes it visible).
//! [`pick_space_destination`] is the write side that turns an unrouted row
//! into a routed one; `REMOVED_FROM_SPACE` ([`record_removed_from_space`])
//! deactivates it (Slack #502's lesson: disconnect actually stops ingest).
//!
//! # Plane-explicit from the first line (design doc §3.8)
//!
//! Every function here takes `(control_pool, tenant_pool, plane)` — never
//! one pool it re-derives a plane from. `routes::google_push`'s
//! interaction-event dispatch has no `Tenant`/HTTP request context (same
//! bootstrap problem `routes::google_push::run_gmail_sync_for_email`
//! documents for Gmail's push path), so its caller passes `state.db` for
//! BOTH pools and `AccountPlane::Personal` — this train's org lift is G4.6's
//! job (D1), and every route in THIS PR refuses under an org
//! `TenancyDecision` outright (`reject_org_google_chat_management`, in
//! `routes::google_chat`). The plane-explicit signature is what lets G4.6
//! wire real org routing later without touching a single function here.
//!
//! # Roster counts are a G4.3 disclosure PREVIEW, not G4.4's seating
//!
//! [`seating_disclosure`] answers "how many people would be seated" for the
//! destination-pick confirmation (contract §5.4) by resolving each
//! `JOINED`, non-bot member's Google `sub` through
//! [`resolve_google_identity_in_account`] — scoped to the account routing
//! THIS space (fixed post-security-review, M3: an earlier version used the
//! cross-account bootstrap resolver here, over-counting anyone connected to
//! CoralSwarm under an unrelated account) — deliberately not gated on
//! `account_members` the way G4.4's actual seat-granting will be (design
//! doc §3.4's `N` = matched users that ALSO pass `EXISTS(account_members
//! ...)`). That gate needs a real seat-grant transaction per member, which
//! is G4.4's job; this module only needs an honest COUNT for a human
//! reading a confirmation prompt before they click. The distinction is
//! called out in the PR body — it is the reason this count can still be a
//! slight over-estimate (counting a person CoralSwarm knows about in this
//! account but who is no longer an `account_members` row — e.g. departed)
//! rather than an exact preview of G4.4's seating pass.
//!
//! # `sub`-keyed resolution has exactly two legitimate shapes here
//!
//! [`resolve_google_identity_by_sub`] (cross-account, control-only
//! `SECURITY DEFINER`) exists ONLY for `ADDED_TO_SPACE`'s account-discovery
//! bootstrap, where no `account_id` is known yet. Every other identity
//! lookup in this module — seating, disclosure — uses the account-scoped
//! [`resolve_google_identity_in_account`] instead, per design doc §3.3/§3.5
//! ("no sub-keyed resolver … never for routing"). Routing/deactivation
//! itself uses neither: it keys on the SPACE via
//! [`resolve_chat_space_accounts`] (D8) — see [`record_removed_from_space`]'s
//! doc comment.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::chat_body::{chat_message_to_item, ChatMessage, ChatSpace};
use super::{google::google_send, ConnectorError, NormalizedItem};
use crate::routes::slack::AccountPlane;

/// `chat.googleapis.com` base — Workspace Events/push notifications aside,
/// every REST call this module makes is a plain, per-member-OAuth-token
/// Chat API v1 call (design doc §2.2-§2.4).
const CHAT_API_BASE: &str = "https://chat.googleapis.com/v1";
#[cfg(test)]
tokio::task_local! { pub(crate) static TEST_CHAT_API_BASE: String; }
fn chat_api_base() -> String {
    #[cfg(test)]
    if let Ok(base) = TEST_CHAT_API_BASE.try_with(Clone::clone) { return base; }
    super::google_transport::endpoint(CHAT_API_BASE)
}


/// Single-page cap for `spaces.list`/`spaces.members.list` — Chat's own
/// documented maximum (§2.2, §2.3). G4.3's routes (a picker UI, a
/// destination-pick disclosure) never need to walk more than one page in
/// practice; multi-page resumable walking is G4.4's backfill/roster-sweep
/// concern (design doc §4.4), not this module's. Documented here rather
/// than silently capping — an account with more than 1000 spaces or a
/// space with more than 1000 members degrades to "the picker doesn't show
/// everything" rather than an error, and that degradation is worth a
/// tracing line at the call site, not a silent truncation.
const LIST_PAGE_SIZE: u32 = 1000;

// ---------------------------------------------------------------------------
// Chat API v1 — REST calls (per-member OAuth token; §2.2-§2.4)
// ---------------------------------------------------------------------------

/// `GET /v1/{name=spaces/*}` — a single space's current metadata. Used by
/// `ADDED_TO_SPACE` handling (to read `spaceType`/`historyState` the
/// interaction event itself may omit or go stale on) and by the
/// destination-pick route's best-effort metadata refresh.
pub(crate) async fn get_space(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
) -> Result<ChatSpace, ConnectorError> {
    let url = format!("{}/{space_id}", chat_api_base());
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!("chat spaces.get failed: {status}")));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("chat spaces.get parse: {e}")))
}

#[derive(Debug, Deserialize)]
struct SpacesListResponse {
    #[serde(default)]
    spaces: Vec<ChatSpace>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: String,
}

/// `GET /v1/spaces` — every space (of any archetype) the CALLING member's
/// own token can see. Follows every page before returning a selection.
pub(crate) async fn list_spaces(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<Vec<ChatSpace>, ConnectorError> {
    let url = format!("{}/spaces", chat_api_base());
    let mut out = Vec::new();
    let mut page = String::new();
    let mut seen = std::collections::HashSet::new();
    loop {
        let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token).query(&[("pageSize", LIST_PAGE_SIZE.to_string()), ("pageToken", page.clone())])).await?;
        if !resp.status().is_success() {
            return Err(ConnectorError::upstream(resp.status(), format!("chat spaces.list failed: {}", resp.status())));
        }
        let parsed: SpacesListResponse = resp.json().await.map_err(|e| ConnectorError::Exchange(format!("chat spaces.list parse: {e}")))?;
        out.extend(parsed.spaces);
        if parsed.next_page_token.is_empty() { return Ok(out); }
        page = parsed.next_page_token;
        if !seen.insert(page.clone()) { return Err(ConnectorError::Exchange("chat spaces.list repeated cursor".into())); }
    }
}

#[derive(Debug, Deserialize)]
struct ChatUserRef {
    #[serde(default)]
    name: String,
    #[serde(rename = "type", default)]
    user_type: String,
    /// **UNVERIFIED at the wire level (design doc §2.4, step-0 item 3).**
    /// Google's own note says a roster read under USER auth populates only
    /// `name` and `type`; whether `displayName` is in practice also present
    /// is a floor-not-ceiling question step 0's probe has not yet answered.
    /// `#[serde(default)]` means an absent field is the empty string, never
    /// a parse error, and [`roster_from_memberships`] treats the empty case
    /// as "no name known" — it changes only the quality of the unmapped
    /// disclosure, never the routing or the seating decision.
    #[serde(rename = "displayName", default)]
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct ChatGroupRef {
    // Presence-only — a group's members are not expandable without a
    // Directory scope this train deliberately does not take (§2.4, §3.4a).
    #[allow(dead_code)]
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
struct ChatMembership {
    #[serde(default)]
    state: String,
    #[serde(default)]
    member: Option<ChatUserRef>,
    #[serde(default, rename = "groupMember")]
    group_member: Option<ChatGroupRef>,
    /// `INTERNAL`, `EXTERNAL`, or `MANAGED_EXTERNAL` (D2, item 42). Empty
    /// when the field is absent — bot rows omit it, and a `HUMAN` row that
    /// omits it is the only case that falls through to directory + domain.
    /// Absence is not itself a guest verdict, and this field does not drop
    /// anyone from capture ([`roster_from_memberships`]).
    #[serde(default)]
    affiliation: String,
}

#[derive(Debug, Deserialize)]
struct MembersListResponse {
    #[serde(default)]
    memberships: Vec<ChatMembership>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// `GET /v1/{parent=spaces/*}/members` — a space's roster. Single page
/// only — see [`LIST_PAGE_SIZE`]'s doc; G4.4's actual seating pass walks
/// every page via [`list_members_all`], this preview does not.
async fn list_members(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
) -> Result<Vec<ChatMembership>, ConnectorError> {
    Ok(list_members_page(http, access_token, space_id, None).await?.memberships)
}

/// One page of `spaces.members.list`, with its `nextPageToken` intact — the
/// paginating half [`list_members_all`] drives.
async fn list_members_page(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
    page_token: Option<&str>,
) -> Result<MembersListResponse, ConnectorError> {
    let mut url = format!("{}/{space_id}/members?pageSize={LIST_PAGE_SIZE}", chat_api_base());
    if let Some(pt) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&urlencoding::encode(pt));
    }
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!("chat spaces.members.list failed: {status}")));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("chat spaces.members.list parse: {e}")))
}

/// What a live `spaces.members.get spaces/{space}/members/app` read says
/// about CoralSwarm's own Chat app in a space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppMembership {
    /// The app is a `JOINED` member of the space.
    Joined,
    /// The space has no app membership (404) or it is not `JOINED`
    /// (`INVITED`, `NOT_A_MEMBER`).
    Absent,
    /// The read itself failed (403/5xx/parse) — says nothing either way.
    Unknown,
}

/// Classify the `members/app` response. Pure so the status/body table is
/// unit-testable without a Chat server.
pub(crate) fn classify_app_membership(status: u16, body: &str) -> AppMembership {
    match status {
        200 => match serde_json::from_str::<ChatMembership>(body) {
            Ok(m) if m.state == "JOINED" => AppMembership::Joined,
            Ok(_) => AppMembership::Absent,
            Err(_) => AppMembership::Unknown,
        },
        404 => AppMembership::Absent,
        _ => AppMembership::Unknown,
    }
}

/// `GET /v1/spaces/{space}/members/app` under the CALLING member's own token
/// (`chat.memberships.readonly`, already in the connect scope set) — the
/// live answer to "is CoralSwarm's Chat app in this space?", independent of
/// whether an `ADDED_TO_SPACE` event was ever received or acted on.
pub(crate) async fn get_app_membership(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
) -> AppMembership {
    let url = format!("{}/{space_id}/members/app", chat_api_base());
    let resp = match google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await {
        Ok(r) => r,
        Err(_) => return AppMembership::Unknown,
    };
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    classify_app_membership(status, &body)
}

/// Named `SPACE`s from a live `spaces.list` that have no ACTIVE watched row —
/// the only ones worth a `members/app` probe. Capped so one listing never
/// fans out into hundreds of Chat API calls.
pub(crate) const APP_PROBE_CAP: usize = 40;

pub(crate) fn spaces_needing_app_probe<'a>(
    live: &'a [ChatSpace],
    watched: &HashMap<String, WatchedSpaceRow>,
    cursor: usize,
) -> Vec<&'a ChatSpace> {
    let eligible: Vec<&ChatSpace> = live
        .iter()
        .filter(|s| s.space_type == "SPACE")
        .filter(|s| !watched.get(&s.name).map(|r| r.active).unwrap_or(false))
        .collect();
    if eligible.is_empty() {
        return Vec::new();
    }
    // Rotate by `cursor` so repeated listings walk past the cap instead of
    // re-probing the same first `APP_PROBE_CAP` spaces forever.
    let start = cursor % eligible.len();
    eligible.iter().cycle().skip(start).take(APP_PROBE_CAP.min(eligible.len())).copied().collect()
}

/// Per-account rotation cursor for [`spaces_needing_app_probe`] (in-process;
/// a restart merely starts the walk over).
static APP_PROBE_CURSORS: std::sync::Mutex<Option<HashMap<Uuid, usize>>> = std::sync::Mutex::new(None);

pub(crate) fn app_probe_cursor(account_id: Uuid) -> usize {
    let g = APP_PROBE_CURSORS.lock().unwrap_or_else(|e| e.into_inner());
    g.as_ref().and_then(|m| m.get(&account_id).copied()).unwrap_or(0)
}

pub(crate) fn advance_app_probe_cursor(account_id: Uuid, by: usize) {
    let mut g = APP_PROBE_CURSORS.lock().unwrap_or_else(|e| e.into_inner());
    let c = g.get_or_insert_with(HashMap::new).entry(account_id).or_insert(0);
    *c = c.wrapping_add(by);
}

// ---------------------------------------------------------------------------
// Identity — the `sub`-keyed, control-only, credential-free resolver (§3.3)
// ---------------------------------------------------------------------------

/// Resolve `(account_id, owner_user_id)` for a bare Google `sub` (as
/// carried on a Chat interaction event's `user.name`, `"users/{id}"` with
/// the prefix already stripped by the caller) via the control-only
/// SECURITY DEFINER resolver (migration `0198`). `None` for an unknown
/// `sub`, or one connected under MORE THAN ONE CoralSwarm account — the
/// function fails closed on that ambiguity itself (see the migration's
/// header); this wrapper never guesses.
///
/// **Identification only — never a credential read.** This resolves WHO a
/// person is, not WHICH token to load; a real credential load always goes
/// through `Surface::token_provider()`-exact, owner-exact lookups
/// elsewhere. `resolve_google_identity_by_sub`'s SQL (migration `0198`)
/// selects `(account_id, owner_user_id)` only — see
/// [`tests::resolve_google_identity_by_sub_function_source_never_selects_a_token_column_pg`]
/// for a source-text guard against a future edit widening that SELECT.
///
/// **Scope, tightened post-security-review (M3): the `ADDED_TO_SPACE`
/// bootstrap ONLY.** This is the one genuinely account-DISCOVERY case
/// (design doc §3.3/§3.5 otherwise forbid a `sub`-keyed resolver — see the
/// migration's header for the full carve-out reasoning). Every other
/// caller that already has an `account_id` in hand — seating counts, the
/// destination-pick disclosure — MUST use
/// [`resolve_google_identity_in_account`] instead, never this function.
pub(crate) async fn resolve_google_identity_by_sub(
    control_pool: &PgPool,
    sub: &str,
) -> Result<Option<(Uuid, Uuid)>, ConnectorError> {
    if sub.is_empty() {
        return Ok(None);
    }
    let row: Option<(Uuid, Uuid)> = sqlx::query_as("SELECT * FROM resolve_google_identity_by_sub($1)")
        .bind(sub)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_google_identity_by_sub: {e}")))?;
    Ok(row)
}

/// The account-scoped identity resolver design doc §3.3 actually specifies
/// (`resolve_google_identity_user_id`, `WHERE account_id = $1`) — the
/// correct helper for any caller (seating counts, the destination-pick
/// disclosure) that already knows which account it's acting for. Unlike
/// [`resolve_google_identity_by_sub`] this is a PLAIN query run inside a
/// scoped transaction, not a `SECURITY DEFINER` function — it needs no
/// elevated privilege because the caller-supplied `account_id` already
/// bounds it, matching the shape `google_gmail::resolve_owner_user_id`
/// uses for the identical account-scoped-lookback role.
///
/// `ORDER BY updated_at DESC LIMIT 1` — deterministic, not merely a
/// plausible default: without it, an account where two different
/// CoralSwarm users each connected the SAME Google identity (an edge case,
/// but a real one — nothing stops it) would resolve to whichever row
/// Postgres happens to return first, silently flipping between them across
/// calls/replans. `provider LIKE 'google%'` (never a credential column) —
/// identification only, same posture as [`resolve_google_identity_by_sub`].
pub(crate) async fn resolve_google_identity_in_account(
    control_pool: &PgPool,
    account_id: Uuid,
    sub: &str,
) -> Result<Option<Uuid>, ConnectorError> {
    if sub.is_empty() {
        return Ok(None);
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_google_identity_in_account scoped tx: {e}")))?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT owner_user_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider LIKE 'google%' AND provider_user_id = $2 \
           AND owner_user_id IS NOT NULL \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(sub)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("resolve_google_identity_in_account: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(u,)| u))
}

/// Resolve which CoralSwarm account(s) hold a container (`SPACE`
/// archetype) row for `space_id`, via the control-only `SECURITY DEFINER`
/// resolver `resolve_chat_space_account` (migration `0198`, added post-
/// security-review, H1). Design doc §3.5 (D8): routing/deactivation keys
/// on the SPACE, never the acting member — unlike
/// [`resolve_google_identity_by_sub`], this deliberately does NOT fail
/// closed on more than one match: two different CoralSwarm accounts CAN
/// each independently hold their own row for the same physical space
/// (the unique index is `(account_id, space_id)`, not global on
/// `space_id`), and a real `REMOVED_FROM_SPACE` event must deactivate
/// EVERY one of them, not pick one.
pub(crate) async fn resolve_chat_space_accounts(
    control_pool: &PgPool,
    space_id: &str,
) -> Result<Vec<Uuid>, ConnectorError> {
    if space_id.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(Uuid,)> = sqlx::query_as("SELECT * FROM resolve_chat_space_account($1)")
        .bind(space_id)
        .fetch_all(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_chat_space_accounts: {e}")))?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Strip the `"users/"` prefix Chat's `User.name` / interaction events
/// carry, leaving the bare Google `sub`. Returns the input unchanged if the
/// prefix is absent (defensive — a malformed/foreign-shaped `name` should
/// never panic here, just fail to resolve downstream).
pub(crate) fn bare_sub(user_name: &str) -> &str {
    user_name.strip_prefix("users/").unwrap_or(user_name)
}

// ---------------------------------------------------------------------------
// The watched-space row (§3.1, §5.2)
// ---------------------------------------------------------------------------

/// A `google_chat_watched_spaces` row — the container (`owner_user_id IS
/// NULL`) shape only; G4.5's mailbox rows are out of this PR's scope.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct WatchedSpaceRow {
    // Present on every read (the RETURNING/SELECT lists below always name
    // it) but not yet consumed by any G4.3 caller — kept for parity with
    // the row's real primary key and for a future direct-addressing
    // consumer (e.g. an audit log entry) rather than re-added later.
    #[allow(dead_code)]
    pub(crate) id: Uuid,
    #[allow(dead_code)]
    pub(crate) account_id: Uuid,
    pub(crate) space_id: String,
    pub(crate) space_type: String,
    pub(crate) mode: Option<String>,
    pub(crate) graph_id: Option<Uuid>,
    pub(crate) reader_user_id: Option<Uuid>,
    pub(crate) display_name: String,
    pub(crate) history_state: String,
    pub(crate) active: bool,
}

/// Org accounts write the watched-space row on the tenant pool.
/// Personal accounts write the consumer pool (`state.db`).
pub(crate) fn added_to_space_account_plane(is_org_tenant: bool) -> AccountPlane {
    if is_org_tenant {
        AccountPlane::Org
    } else {
        AccountPlane::Personal
    }
}

/// `ADDED_TO_SPACE` — create (or reactivate) the watched-space row for
/// `space`. Idempotent, with two distinct reactivation shapes (founder
/// decision G4-Q6, 2026-08-25):
///
/// * **A row that was already `active`** (a redundant/duplicate
///   `ADDED_TO_SPACE` for a space that was never removed) refreshes
///   display metadata only — an existing destination pick is left exactly
///   as it was (contract I2: fixed at authorisation time).
/// * **A row that was `active = false`** (a genuine re-add AFTER a
///   [`record_removed_from_space`] removal) resets to the **unrouted**
///   state — `mode`/`graph_id` cleared back to `NULL` — rather than
///   silently resurrecting the prior destination. Rationale: the space's
///   roster may have turned over completely while the app was out of it,
///   so resuming silently would make a new, un-consulted set of people
///   inherit a consent they never gave. A human must pick a destination
///   again and see the §5.4 seating disclosure fresh. The full lifecycle
///   is therefore: add → unrouted → routed → removed (deactivated) →
///   re-add → **unrouted again**, not a loop back to routed.
///
/// **Only the `SPACE` archetype reaches this table via `ADDED_TO_SPACE`.**
/// Whether the Chat app can even be added to a `GROUP_CHAT` is UNVERIFIED
/// (design doc §5.5) — if it ever is, this function refuses rather than
/// attempting an insert the table's own archetype CHECK would reject
/// anyway (a `GROUP_CHAT`/`DIRECT_MESSAGE` row must be `mode='single'` with
/// a named `owner_user_id`, neither of which an app-membership event can
/// supply — G4.5's opt-in route is the only writer for that archetype).
///
/// `adder_account` is the `(account_id, owner_user_id)` pair already
/// resolved (via [`resolve_google_identity_by_sub`]) from the interaction
/// event's `user.name` — `None` when the adding member has no resolvable
/// CoralSwarm identity yet (not connected to any Google surface). Per D6,
/// the row is still created either way: the app's membership is the
/// authorising act regardless of whether we can yet identify who performed
/// it, and `reader_user_id` is simply left `NULL` until a later
/// `pick_space_destination` call (or, eventually, G4.4's failover
/// election) supplies a connected reader.
pub(crate) async fn record_added_to_space(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    plane: AccountPlane,
    account_id: Uuid,
    adder_user_id: Option<Uuid>,
    space: &ChatSpace,
) -> Result<Option<WatchedSpaceRow>, sqlx::Error> {
    if space.space_type != "SPACE" {
        tracing::warn!(
            space_id = %space.name,
            space_type = %space.space_type,
            "google chat: ADDED_TO_SPACE for a non-SPACE archetype — refusing (G4.5's opt-in \
             route is the only legitimate writer for GROUP_CHAT/DIRECT_MESSAGE rows)"
        );
        return Ok(None);
    }

    // CS-432 (D17 point 4): `control_pool` was "kept for the plane-explicit
    // signature (§3.8) — not read yet" before this fix — this is that read.
    // For a genuine org tenant plane, check (once per space per 6h, see
    // `warn_if_chat_space_plane_mismatch`) whether a row for this exact
    // (account, space) is ALREADY sitting on CONTROL — every write before
    // this fix landed on whichever pool `handle_chat_added_to_space` was
    // handed, unconditionally `state.db`, so an org that used Chat before
    // CS-432 shipped has its row on the WRONG plane, invisible to every
    // org-plane reader (`tick_org`, the REST picker, the destination pick —
    // D17 point 4's trace). This story deliberately does not migrate that
    // row (a reset-to-unrouted vs. copy-forward choice with product
    // consequences this ticket doesn't take on) — it only makes the
    // mismatch OBSERVABLE.
    if plane == AccountPlane::Org {
        warn_if_chat_space_plane_mismatch(control_pool, account_id, &space.name).await;
    }

    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("record_added_to_space scoped tx: {e}")))?;

    let row: WatchedSpaceRow = sqlx::query_as(
        r#"
        INSERT INTO google_chat_watched_spaces
            (account_id, space_id, space_type, display_name, history_state, reader_user_id, active)
        VALUES ($1, $2, 'SPACE', $3, $4, $5, true)
        ON CONFLICT (account_id, space_id) WHERE owner_user_id IS NULL
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            history_state = EXCLUDED.history_state,
            active = true,
            -- Founder decision G4-Q6 (2026-08-25): a re-add after a
            -- REMOVAL resets to unrouted — never silently resurrects the
            -- prior destination. `google_chat_watched_spaces.active` here
            -- reads the PRE-UPDATE value (Postgres evaluates a DO UPDATE
            -- SET's right-hand side against the conflicting row as it
            -- stood before this statement): if the row was already ACTIVE
            -- (a redundant/duplicate ADDED_TO_SPACE for a space that was
            -- never removed), an existing pick is preserved as before;
            -- if it was INACTIVE (a genuine re-add), mode/graph_id reset
            -- to NULL so a human must pick again and see the §5.4 seating
            -- disclosure fresh — the space's roster may have turned over
            -- completely while it was removed.
            mode = CASE WHEN google_chat_watched_spaces.active THEN google_chat_watched_spaces.mode ELSE NULL END,
            graph_id = CASE WHEN google_chat_watched_spaces.active THEN google_chat_watched_spaces.graph_id ELSE NULL END,
            last_resolved_at = now()
        RETURNING id, account_id, space_id, space_type, mode, graph_id, reader_user_id,
                  display_name, history_state, active
        "#,
    )
    .bind(account_id)
    .bind(&space.name)
    .bind(&space.display_name)
    .bind(&space.history_state)
    .bind(adder_user_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(row))
}

/// Per-(account, space) cooldown for [`warn_if_chat_space_plane_mismatch`] —
/// CS-432 (D17 point 4). Same in-process, claim-as-it-reports, 6-hour shape
/// as `connectors::slack::UNMATCHED_NOTICE_COOLDOWN`/
/// `microsoft::ROSTER_UNAVAILABLE_HEALTH_COOLDOWN`: bounded and small rather
/// than a schema change, and correct for the same reason those two guard a
/// repeat-event flood — a redelivered `ADDED_TO_SPACE`, or every subsequent
/// push for a space stuck on the wrong plane, would otherwise re-fire this
/// warning without limit.
///
/// **Gates the CHECK itself, not only a confirmed mismatch.** `record_added_
/// to_space`/`run_chat_event` call [`warn_if_chat_space_plane_mismatch`] for
/// EVERY org-plane `(account, space)` they touch, whether or not a stale
/// control row turns out to exist — the round-trip
/// [`chat_space_row_exists`] runs is what this bounds, same as the cost
/// `should_send_unmatched_notice`'s own callers pay on every message in a
/// busy channel. So a fresh space with no leftover consumes this slot too;
/// only [`chat_space_row_exists`]'s own return value distinguishes "checked,
/// found nothing" from "checked, found a mismatch" — this cooldown alone
/// proves only that a check ran once for the window.
fn chat_plane_mismatch_cooldown(
) -> &'static std::sync::Mutex<std::collections::HashMap<(Uuid, String), std::time::Instant>> {
    static C: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<(Uuid, String), std::time::Instant>>,
    > = std::sync::OnceLock::new();
    C.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Same window as `connectors::slack::UNMATCHED_NOTICE_COOLDOWN` — long
/// enough that a busy space is not the loudest thing in the logs, short
/// enough that a fix (or a re-add that lands on the right plane) is
/// reflected within a shift.
const CHAT_PLANE_MISMATCH_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(6 * 60 * 60);

/// `true` at most once per [`CHAT_PLANE_MISMATCH_COOLDOWN`] per (account,
/// space). Claims the slot as it reports, so concurrent callers for the same
/// space cannot both pass.
pub(crate) fn should_warn_chat_plane_mismatch(account_id: Uuid, space_id: &str) -> bool {
    let now = std::time::Instant::now();
    let mut guard = match chat_plane_mismatch_cooldown().lock() {
        Ok(g) => g,
        // A poisoned mutex means some other task panicked mid-update.
        // Staying quiet is the safe direction for an observability-only warning.
        Err(_) => return false,
    };
    // Opportunistic sweep — this map is keyed by (account, space) and would
    // otherwise grow for the lifetime of the process.
    guard.retain(|_, sent| now.duration_since(*sent) < CHAT_PLANE_MISMATCH_COOLDOWN);

    match guard.get(&(account_id, space_id.to_string())) {
        Some(sent) if now.duration_since(*sent) < CHAT_PLANE_MISMATCH_COOLDOWN => false,
        _ => {
            guard.insert((account_id, space_id.to_string()), now);
            true
        }
    }
}

/// CS-432 (D17 point 4): for an account whose tenancy resolves to its own
/// tenant database, detect a `google_chat_watched_spaces` row LEFT BEHIND on
/// the CONTROL plane from before this fix shipped. Every write before
/// CS-432 landed on `state.db` regardless of tenancy
/// (`routes::google_push::handle_chat_added_to_space` hardcoded
/// `AccountPlane::Personal` and its own pool for both the control and
/// tenant argument), so an ordinary org that was already using Chat has its
/// row on the WRONG plane — invisible to every org-plane reader (`tick_org`,
/// the REST picker, the destination pick).
///
/// **Read-only, best-effort, never fails the caller.** This runs on the hot
/// path of both `record_added_to_space` and the push-event space resolver;
/// a failed check here must never block the write/read it is only trying to
/// make observable. Scoped via `begin_tenant_system_scoped` like every other
/// read against `google_chat_watched_spaces` in this module — RLS applies to
/// this table on control exactly as it does on a tenant pool.
pub(crate) async fn warn_if_chat_space_plane_mismatch(control_pool: &PgPool, account_id: Uuid, space_id: &str) {
    if !should_warn_chat_plane_mismatch(account_id, space_id) {
        return;
    }
    match chat_space_row_exists(control_pool, account_id, space_id).await {
        Ok(true) => tracing::warn!(
            %account_id, space_id = %space_id, connector_health = true,
            "google chat: a watched-space row for this tenant-DB org account exists on the \
             CONTROL plane, not its own tenant database — every org-plane reader (tick_org, the \
             REST picker, the destination pick) looks on the tenant plane and will never see it \
             (D17 point 4). Not migrated by CS-432 by design; needs a human decision on \
             reset-to-unrouted vs. copy-forward before this space can route again."
        ),
        Ok(false) => {}
        Err(e) => tracing::warn!(
            %account_id, space_id = %space_id, error = %e,
            "google chat: plane-mismatch check itself failed — could not confirm whether a \
             stale control-plane row exists for this tenant-DB org account"
        ),
    }
}

/// The container-row existence check [`warn_if_chat_space_plane_mismatch`]
/// runs — split out so it is directly testable without needing to observe a
/// `tracing::warn!` call. Scoped via `begin_tenant_system_scoped` like every
/// other read against `google_chat_watched_spaces` in this module (RLS
/// applies to this table on control exactly as it does on a tenant pool);
/// a scoping failure is folded into `Err` rather than a silent `false` so
/// the caller's own error arm (not this one's `Ok(false)`) reports it.
async fn chat_space_row_exists(pool: &PgPool, account_id: Uuid, space_id: &str) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("chat_space_row_exists scoped tx: {e}")))?;
    let exists: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NULL LIMIT 1",
    )
    .bind(account_id)
    .bind(space_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(exists.is_some())
}

/// `REMOVED_FROM_SPACE` — deactivate ONE account's watched-space row for
/// `space_id`, stopping ingest immediately (Slack #502's lesson). Returns
/// `true` iff a row was found and deactivated. **Never deletes the row** —
/// a destination pick (if one was ever made) stays on record, so a later
/// re-add ([`record_added_to_space`]) can resume feeding the SAME ocean
/// rather than starting the picker over.
///
/// **Per-account, not the fan-out itself (H1 fix, this PR round 2).** The
/// physical Chat space may be independently watched by MORE THAN ONE
/// CoralSwarm account (`resolve_chat_space_accounts` can return several) —
/// this function deactivates exactly the one `account_id` it's given.
/// `routes::google_push::handle_chat_removed_from_space` is the fan-out:
/// it resolves every account via [`resolve_chat_space_accounts`] (keyed on
/// the SPACE, per design doc §3.5/D8 — never on the removing member's own
/// identity, which was this function's original, incorrect caller and is
/// still worth naming here so nobody "fixes" this back to a `sub`-keyed
/// call) and calls this function once per account.
pub(crate) async fn record_removed_from_space(
    tenant_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("record_removed_from_space scoped tx: {e}")))?;
    let updated: Option<(Uuid,)> = sqlx::query_as(
        r#"
        UPDATE google_chat_watched_spaces
        SET active = false, last_resolved_at = now()
        WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NULL
        RETURNING id
        "#,
    )
    .bind(account_id)
    .bind(space_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(updated.is_some())
}

/// Read every container row this account has (routed or not, active or
/// not) — the local half of `GET /v1/connectors/google/chat/spaces`
/// (`routes::google_chat` LEFT-JOINs this against a live `spaces.list`).
/// Bumps nothing — unlike the Slack precedent, this is a listing read, not
/// a per-space destination resolution (that bump happens in
/// [`pick_space_destination`] and [`record_added_to_space`] instead).
pub(crate) async fn list_watched_spaces(
    tenant_pool: &PgPool,
    account_id: Uuid,
) -> Result<HashMap<String, WatchedSpaceRow>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("list_watched_spaces scoped tx: {e}")))?;
    let rows: Vec<WatchedSpaceRow> = sqlx::query_as(
        r#"
        SELECT id, account_id, space_id, space_type, mode, graph_id, reader_user_id,
               display_name, history_state, active
        FROM google_chat_watched_spaces
        WHERE account_id = $1 AND owner_user_id IS NULL
        "#,
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|r| (r.space_id.clone(), r)).collect())
}

// ---------------------------------------------------------------------------
// Destination pick (§3.2, I2)
// ---------------------------------------------------------------------------

/// A destination pick submitted to [`pick_space_destination`] — the
/// container-archetype analogue of `connectors::slack::PickedDestination`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PickedDestination {
    Single(Uuid),
    FanOut,
}

/// The result of [`pick_space_destination`] — same "saved" vs
/// "already-routed" distinction as `connectors::slack::EnsureOutcome`, for
/// the same reason (I2's no-re-point guarantee means a second pick is
/// ALWAYS the latter; reporting it as success either way would lie to
/// whoever just clicked).
#[derive(Debug, Clone)]
pub(crate) struct PickOutcome {
    pub(crate) row: WatchedSpaceRow,
    pub(crate) newly_routed: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PickError {
    /// No `google_chat_watched_spaces` row for this `(account_id,
    /// space_id)` — the Chat app has not been added to this space (D6), so
    /// there is nothing to route yet.
    #[error("space not connected — add the CoralSwarm Chat app to it first")]
    NotConnected,
    #[error("that ocean does not exist or does not belong to your account")]
    GraphNotOwned,
    #[error("\"everyone's individual oceans\" is only available in an organization workspace")]
    FanoutRequiresOrgPlane,
    #[error("database error: {0}")]
    Db(String),
}

impl From<sqlx::Error> for PickError {
    fn from(e: sqlx::Error) -> Self {
        PickError::Db(e.to_string())
    }
}

/// Persist a human's explicit destination pick for an already-watched
/// space (§3.2). Structurally `connectors::slack::ensure_slack_watched_channel`,
/// narrowed to an UPDATE (the row already exists from
/// [`record_added_to_space`] — there is no "insert on first pick" path for
/// the container archetype, unlike Slack's channel table).
///
/// **Ownership check, same shape as Slack's** (`ensure_slack_watched_channel`'s
/// doc comment): `PickedDestination::Single`'s `graph_id` is verified to
/// belong to `account_id` INSIDE the same scoped transaction the UPDATE
/// runs in, before the write — never trusted bare. `FanOut` is refused
/// outright off the Personal plane (a personal account has no "everyone"
/// to fan out to — identical reasoning to Slack's own plane guard).
///
/// The UPDATE's `WHERE ... AND mode IS NULL` clause is contract I2 made
/// structural: a row that is ALREADY routed simply does not match, and
/// this function re-reads the existing row to report `newly_routed:
/// false` rather than silently re-pointing it.
///
/// **`AND active = true`** (security-review finding L4, this PR round 2):
/// without it, a space the app was REMOVED from (`active = false`,
/// [`record_removed_from_space`]) could still be routed — the row still
/// matched `mode IS NULL`, so a stale unrouted row from before removal
/// would happily accept a destination pick and report
/// `{"newly_routed": true}`, even though D6's own gate (the app must
/// currently be a member of the space) no longer holds. Compounds H1:
/// before that fix, a removal an unconnected third party performed never
/// even flipped `active` in the first place. Now an inactive space is
/// refused the same way a never-watched one is
/// ([`PickError::NotConnected`]) — see the re-read arm below.
pub(crate) async fn pick_space_destination(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    plane: AccountPlane,
    account_id: Uuid,
    space_id: &str,
    destination: PickedDestination,
) -> Result<PickOutcome, PickError> {
    // Kept for the plane-explicit signature (§3.8), not read yet: the
    // container archetype's ownership check and UPDATE both run entirely
    // on `tenant_pool` (`graphs`/`google_chat_watched_spaces` are both
    // tenant-tree tables). `control_pool` becomes load-bearing once a
    // `FanOut` pick needs a live `account_members` read to materialize its
    // seat count (`connectors::slack::resolve_org_fanout_destinations`'s
    // own two-pool shape is the precedent) — not this PR's scope.
    let _ = control_pool;
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| PickError::Db(format!("pick_space_destination scoped tx: {e}")))?;

    let (mode, graph_id_param): (&'static str, Option<Uuid>) = match destination {
        PickedDestination::Single(graph_id) => {
            let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM graphs WHERE id = $1 AND account_id = $2)")
                .bind(graph_id)
                .bind(account_id)
                .fetch_one(&mut *tx)
                .await?;
            if !owned {
                tx.rollback().await.ok();
                return Err(PickError::GraphNotOwned);
            }
            ("single", Some(graph_id))
        }
        PickedDestination::FanOut => {
            if plane != AccountPlane::Org {
                tx.rollback().await.ok();
                return Err(PickError::FanoutRequiresOrgPlane);
            }
            ("fanout", None)
        }
    };

    let updated: Option<WatchedSpaceRow> = sqlx::query_as(
        r#"
        UPDATE google_chat_watched_spaces
        SET mode = $3, graph_id = $4, last_resolved_at = now()
        WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NULL AND mode IS NULL
          AND active = true
        RETURNING id, account_id, space_id, space_type, mode, graph_id, reader_user_id,
                  display_name, history_state, active
        "#,
    )
    .bind(account_id)
    .bind(space_id)
    .bind(mode)
    .bind(graph_id_param)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    match updated {
        Some(row) => Ok(PickOutcome { row, newly_routed: true }),
        None => {
            // Never watched, already routed, or inactive (removed) — re-read
            // to tell them apart (same posture as
            // `ensure_slack_watched_channel`'s `None` arm).
            let mut tx2 = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
                .await
                .map_err(|e| PickError::Db(format!("pick_space_destination re-read tx: {e}")))?;
            let existing: Option<WatchedSpaceRow> = sqlx::query_as(
                r#"
                SELECT id, account_id, space_id, space_type, mode, graph_id, reader_user_id,
                       display_name, history_state, active
                FROM google_chat_watched_spaces
                WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NULL
                "#,
            )
            .bind(account_id)
            .bind(space_id)
            .fetch_optional(&mut *tx2)
            .await?;
            tx2.rollback().await.ok();
            match existing {
                // Inactive (the app was removed, L4) reads the same as
                // never-watched — there is nothing currently to route.
                Some(row) if !row.active => Err(PickError::NotConnected),
                Some(row) => Ok(PickOutcome { row, newly_routed: false }),
                None => Err(PickError::NotConnected),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Seating disclosure preview (§5.4) — see this module's doc for the
// "preview, not G4.4's seating" caveat.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub(crate) struct SeatingDisclosure {
    /// `JOINED`, non-bot, non-group members.
    pub(crate) total_humans: usize,
    /// Of `total_humans`, how many resolve to SOME CoralSwarm identity
    /// (design doc §3.3's "identification only" caveat applies — see this
    /// module's doc comment).
    pub(crate) matched: usize,
    /// `total_humans - matched`, derived (never an independent count — the
    /// `slack.rs:593-596` bug class this design doc's §3.4 calls out by
    /// name).
    pub(crate) unmapped: usize,
    /// `JOINED` `groupMember` entries — Google Groups, unexpandable without
    /// a Directory scope this train does not take (§2.4, §3.4a). Counted
    /// SEPARATELY from `unmapped`: "cannot enumerate at all" is a
    /// different statement from "these people have no CoralSwarm account".
    pub(crate) group_memberships: usize,
    /// `false` when the roster read itself failed (network/auth/parse) —
    /// an unknown roster must never be silently reported as an empty one
    /// (design doc §3.4's standing rule). Callers show "unavailable", never
    /// zero, when this is `false`.
    pub(crate) available: bool,
}

/// Best-effort roster read + count for the destination-pick confirmation.
/// Never returns `Err` for an unreadable roster — degrades to
/// `SeatingDisclosure { available: false, .. }` instead, since a failed
/// disclosure preview must not block a human's explicit destination pick
/// (I2's authorisation act is the pick itself, not the count).
///
/// **`account_id`-scoped identity resolution (security-review finding M3,
/// this PR round 2).** The original version resolved each roster member's
/// `sub` via [`resolve_google_identity_by_sub`] — the CROSS-ACCOUNT
/// bootstrap resolver — which counted a member as `matched` if their `sub`
/// resolved under ANY CoralSwarm account, not specifically THIS one. That
/// over-states contract §5.4's mandatory "how many will be seated"
/// disclosure: a roster member connected to CoralSwarm under a wholly
/// unrelated account (never a member of `account_id`, never seatable
/// there) still counted as matched. Now uses
/// [`resolve_google_identity_in_account`], scoped to the account actually
/// routing this space — matching design doc §3.3's specified helper.
pub(crate) async fn seating_disclosure(
    http: &reqwest::Client,
    control_pool: &PgPool,
    account_id: Uuid,
    access_token: &str,
    space_id: &str,
) -> SeatingDisclosure {
    let memberships = match list_members(http, access_token, space_id).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(space_id, error = %e, "google chat: roster read failed for seating disclosure — reporting unavailable, not zero");
            return SeatingDisclosure::default();
        }
    };

    let mut total_humans = 0usize;
    let mut matched = 0usize;
    let mut group_memberships = 0usize;
    for m in &memberships {
        if m.state != "JOINED" {
            continue;
        }
        if m.group_member.is_some() {
            group_memberships += 1;
            continue;
        }
        let Some(member) = &m.member else { continue };
        // `eq_ignore_ascii_case`, matching `roster_from_memberships`'s own
        // test exactly (security review, L3). Google returns uppercase today,
        // so this is not a live defect — but the disclosure preview and the
        // seating pass are meant to apply ONE rule, and two spellings of it
        // is how they drift into disagreeing about who is a bot.
        if member.user_type.eq_ignore_ascii_case("BOT") {
            continue;
        }
        // Seat preview, not capture. EXTERNAL / MANAGED_EXTERNAL are not
        // seated or invited (item 42). They stay in [`roster_from_memberships`]
        // so message capture still sees them.
        if is_external_affiliation(&m.affiliation)
            || (!m.affiliation.trim().is_empty() && !m.affiliation.trim().eq_ignore_ascii_case("INTERNAL"))
        {
            continue;
        }
        total_humans += 1;
        let sub = bare_sub(&member.name);
        match resolve_google_identity_in_account(control_pool, account_id, sub).await {
            Ok(Some(_)) => matched += 1,
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(space_id, %sub, error = %e, "google chat: identity resolution failed during seating disclosure — counting as unmapped");
            }
        }
    }

    SeatingDisclosure {
        total_humans,
        matched,
        unmapped: total_humans.saturating_sub(matched),
        group_memberships,
        available: true,
    }
}

// ===========================================================================
// G4.4 — the INGEST half: subscription, cursors, backfill, catch-up, roster,
// seating, deletion propagation.
//
// Everything below this line is gsuite Train G4 PR 4 (`.scratch/
// gsuite-enterprise/g4-chat-design.md` §4.1/§4.4/§3.4/§3.5/§5.3, §7's
// "G4.4"). It is structurally `connectors::google_gmail`'s ingest half with
// three differences the design names explicitly:
//
//  1. **The change-detection cursor is PER SPACE, not per connection.** One
//     Workspace Events subscription per connected member covers every space
//     they belong to (`//chat.googleapis.com/spaces/-`, §4.1), so
//     `google_watch_channels.sync_token` is unused for chat — the watermark
//     lives on `google_chat_watched_spaces.sync_token`, one per container
//     (§4.4). `backfill_cursor`/`backfill_complete` sit beside it.
//  2. **Routing keys on the SPACE, never the subscriber** (D8, §3.5). N
//     members' subscriptions may each deliver the same message; the
//     destination is a property of the space. See `routes::google_push`'s
//     Chat arm.
//  3. **The write is Path 1, not Path 2** (§3.9/D10). A space's destination
//     may be a team ocean, which `ingest::is_own_writable_ocean`
//     refuses by design, so `ingest_atom` is not reachable for it — the
//     space path hand-rolls the Slack-shaped multi-destination write
//     ([`ingest_space_items`]). On the PERSONAL plane a space's destination
//     is `kind='personal'` and WOULD pass that gate, which is exactly why
//     the design (§3.8, line 524) forbids shipping the Path-2 shape here:
//     the org lift (G4.6) would then be a rewrite rather than a gate flip.
//
// Plane discipline (§3.8): `google_watch_channels` touches take a
// `control_pool`, `google_chat_watched_spaces`/`graphs`/`atoms` touches take
// a `content_pool`, and no function here re-derives a plane from a pool it
// was handed. Under Personal tenancy the two are the same physical database
// (`tenancy::extractor::Tenant`'s own doc), so today's behaviour is
// byte-identical either way — the split is what lets G4.6 wire real org
// routing without touching a signature here.
// ===========================================================================

/// Workspace Events API base — `subscriptions.create` lives here, NOT on
/// `chat.googleapis.com` (a different service with a different quota).
const WORKSPACE_EVENTS_BASE: &str = "https://workspaceevents.googleapis.com/v1";
#[cfg(test)]
tokio::task_local! { pub(crate) static TEST_WORKSPACE_EVENTS_BASE: String; }
pub(crate) fn workspace_events_base() -> String {
    #[cfg(test)]
    if let Ok(base) = TEST_WORKSPACE_EVENTS_BASE.try_with(Clone::clone) { return base; }
    super::google_transport::endpoint(WORKSPACE_EVENTS_BASE)
}

/// The subscription target (§2.6, §4.1): **every space the calling member is
/// a member of**, in one subscription. Documented by Google as
/// user-authentication-only, which is exactly the auth model this train has.
/// Membership churn therefore needs no subscription churn at all.
pub(crate) const CHAT_ALL_SPACES_TARGET: &str = "//chat.googleapis.com/spaces/-";

/// The four subscribed event types (§4.1). `message.v1.updated` is
/// DELIBERATELY absent — §5.3's "edits reported upstream, not honoured in
/// v1" declaration, pinned by [`tests::subscribed_event_types_exclude_updates`].
/// `reaction.*` / `space.*` / read-state events are equally deliberately
/// absent: a narrower event set is a smaller consent story, and nothing
/// downstream consumes them.
pub(crate) const CHAT_SUBSCRIBED_EVENT_TYPES: [&str; 4] = [
    "google.workspace.chat.message.v1.created",
    "google.workspace.chat.message.v1.deleted",
    "google.workspace.chat.membership.v1.created",
    "google.workspace.chat.membership.v1.deleted",
];

/// `atoms.source_type` for a Chat message — BOTH archetypes (§3.6, D2: the
/// archetype lives in the routing, not the label). Registered in all five
/// display/chunker maps by G4.2; never `"chat"`, which `mcp/tools/write.rs`
/// already claims for hand-written MCP notes.
pub(crate) const SOURCE_TYPE: &str = "google_chat";

/// Page size for one `spaces.messages.list` request — Chat's documented
/// maximum (§2.2). Not a cap on total messages per pass; see
/// [`SYNC_PASS_BUDGET`].
const MESSAGES_PAGE_SIZE: u32 = 1000;

/// Page size for one `spaces.spaceEvents.list` request. Deliberately far
/// below the messages page size: a catch-up pass re-fetches every event's
/// message individually (C7: "treat pushes as triggers, not as data"), so
/// each page costs N round trips, not one.
const SPACE_EVENTS_PAGE_SIZE: u32 = 100;

/// Max messages accumulated in ONE backfill/catch-up pass before persisting
/// a resume cursor and returning — same value and same reasoning as
/// `google_gmail::SYNC_PASS_BUDGET` / `google_calendar::SYNC_PASS_BUDGET`: a
/// bounded worst case per tick, never an unbounded walk. The design's "no
/// page cap" (`plan.md:86`) is about the BACKFILL being resumable to
/// completion, not about doing it all in one tick.
const SYNC_PASS_BUDGET: usize = 2000;

/// Re-create the subscription if it expires within this margin. Same value
/// and reasoning as `google_gmail::CHANNEL_LIVE_MARGIN_MINUTES`.
const CHANNEL_LIVE_MARGIN_MINUTES: i64 = 30;

/// `spaces.spaceEvents.list` retention (§2.7): *"You can list events that
/// occurred up to 28 days ago."* A catch-up watermark older than this is
/// floored here rather than sent upstream, where it would be rejected — the
/// events before it are unrecoverable, which is precisely why a lapsed
/// subscription is a correctness alert (`workers::google_watch_renewal::
/// tick_backstop`) and not merely a hygiene one.
const SPACE_EVENTS_RETENTION_DAYS: i64 = 28;

/// Max roster pages walked in one [`list_members_all`] call. A total budget,
/// not a per-request size: *"an empty channel and an inaccessible one must
/// never look identical"* (`connectors::slack::fetch_conversation_members`'s
/// own rule) — hitting this cap is an `Err`, never a silently short roster,
/// because a short roster would under-count the §5.4 disclosure and
/// under-seat the space.
const MEMBERS_MAX_PAGES: usize = 25;

async fn scoped_tx(
    pool: &PgPool,
    account_id: Uuid,
    what: &str,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, ConnectorError> {
    crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("{what} scoped tx: {e}")))
}

// ---------------------------------------------------------------------------
// §4.1 — one Workspace Events subscription per connected member
// ---------------------------------------------------------------------------

/// Whether a live (not imminently expiring) Chat subscription row already
/// exists for this member — the cheap pre-check mirroring
/// `google_gmail::watch_is_live`.
///
/// Keyed `(account_id, surface='chat', resource_id=<the member's Google
/// `sub`>)`. **`resource_id` is the `sub`, `channel_id` is the subscription
/// name** — design doc §1.2's resolution of the convention conflict between
/// `0180`'s header (which called `resource_id` the *target* resource name,
/// identical for every member and therefore useless as a key) and the merged
/// `renew_chat` (which PATCHed `resource_id` as if it were the subscription
/// name). `resource_id` must be a STABLE upsert key so `0180:96`'s
/// `UNIQUE (account_id, surface, resource_id)` means "one row per member";
/// a subscription name is not stable (Workspace Events mints a fresh one
/// every time an expired subscription is replaced, so keying on it would
/// accumulate a dead row per lapse), while the `sub` survives both
/// re-creation and a Workspace primary-address change. The subscription name
/// goes in `channel_id`, where `0182:93`'s partial unique index additionally
/// enforces one row per live subscription globally — correct, since
/// subscription names ARE globally unique.
async fn watch_is_live(control_pool: &PgPool, account_id: Uuid, sub: &str) -> Result<bool, ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "chat watch_is_live").await?;
    let row: Option<(DateTime<Utc>,)> = sqlx::query_as(
        "SELECT expires_at FROM google_watch_channels \
         WHERE account_id = $1 AND surface = 'chat' AND resource_id = $2",
    )
    .bind(account_id)
    .bind(sub)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("chat watch_is_live: {e}")))?;
    tx.rollback().await.ok();
    Ok(row
        .map(|(expires_at,)| expires_at > Utc::now() + chrono::Duration::minutes(CHANNEL_LIVE_MARGIN_MINUTES))
        .unwrap_or(false))
}

/// Insert or refresh this member's `google_watch_channels` row.
///
/// The `ON CONFLICT` arm deliberately does **not** touch `sync_token` or
/// `backfill_page_token` — byte-identical reasoning to
/// `google_gmail::upsert_watch` (`google_gmail.rs:220`-`:225`): a renewal
/// call must never clobber a still-valid cursor with a stale baseline. For
/// chat those two columns are unused entirely (the real cursors are
/// per-space, on `google_chat_watched_spaces`), so the rule is belt-and-
/// suspenders here rather than load-bearing — but writing the ON CONFLICT
/// arm the same way in both connectors is what stops a future edit from
/// having to re-derive which columns are safe to overwrite.
///
/// `channel_id` (the subscription name) IS refreshed on conflict: a
/// re-created subscription must UPDATE the member's existing row rather than
/// add a second one, which is design doc §1.2's whole argument made
/// falsifiable — see
/// [`tests::recreated_subscription_updates_the_same_watch_row_pg`].
async fn upsert_watch(
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    sub: &str,
    subscription_name: &str,
    topic_name: &str,
    expires_at: DateTime<Utc>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(control_pool, account_id, "chat upsert_watch").await?;
    sqlx::query(
        r#"
        INSERT INTO google_watch_channels
            (account_id, surface, resource_id, channel_id, owner_user_id, topic_name, expires_at, updated_at)
        VALUES ($1, 'chat', $2, $3, $4, $5, $6, now())
        ON CONFLICT (account_id, surface, resource_id)
        DO UPDATE SET channel_id = EXCLUDED.channel_id,
                      owner_user_id = EXCLUDED.owner_user_id,
                      topic_name = EXCLUDED.topic_name,
                      expires_at = EXCLUDED.expires_at,
                      last_error = NULL,
                      last_error_at = NULL,
                      updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(sub)
    .bind(subscription_name)
    .bind(owner_user_id)
    .bind(topic_name)
    .bind(expires_at)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("chat upsert_watch: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("chat upsert_watch commit: {e}")))?;
    Ok(())
}

/// The `subscriptions.create` request body (§4.1). Built by a named function
/// rather than inline so [`tests::subscription_body_omits_payload_options`]
/// can assert the one thing that must never drift.
///
/// **`payloadOptions` is deliberately absent — D5, §4.2, and this is the
/// single most consequential constraint in the train.** Including resource
/// data in the payload drops the maximum subscription TTL from **7 days to
/// 4 hours** (the 24-hour middle option requires domain-wide delegation,
/// which plan decision 3 rejects). `workers::google_watch_renewal`'s
/// `RENEW_WINDOW_HOURS = 24` + `BACKSTOP_INTERVAL = 6h` cadence is correct
/// under a 7-day TTL and catastrophically wrong under a 4-hour one — every
/// deploy window would become a data-loss window. And the payload would buy
/// content we are contractually required to re-fetch anyway (contract §4's
/// C7 row: *"treat pushes as triggers, not as data"*). Risk 2 in the design
/// doc's risk list asks for this to be pinned by a test, not just a comment.
fn subscription_request_body(topic_name: &str) -> serde_json::Value {
    serde_json::json!({
        "targetResource": CHAT_ALL_SPACES_TARGET,
        "eventTypes": CHAT_SUBSCRIBED_EVENT_TYPES,
        "notificationEndpoint": { "pubsubTopic": topic_name },
        // "0s" = the API maximum, whatever that maximum currently is — the
        // same literal `renew_chat`'s PATCH already sends.
        "ttl": "0s",
    })
}

/// Pull `(subscription name, expireTime)` out of a finished Workspace Events
/// `Operation`. Pure — unit-tested against
/// `tests/fixtures/google_chat/subscriptions_create_response.json`, which is
/// a real `Operation` envelope (`done: true`, `response` = the
/// `Subscription`), NOT a bare `Subscription`: the same #674-round-1
/// finding-4 contract `workers::google_watch_renewal::
/// expire_time_from_operation` already documents for the renewal PATCH.
pub(crate) fn subscription_from_operation(
    op: &crate::workers::google_watch_renewal::Operation,
) -> Result<(String, DateTime<Utc>), ConnectorError> {
    if let Some(err) = &op.error {
        return Err(ConnectorError::Exchange(format!(
            "workspace events subscriptions.create operation {} failed: {err}",
            op.name
        )));
    }
    let response = op.response.as_ref().ok_or_else(|| {
        ConnectorError::Exchange(format!(
            "workspace events subscriptions.create operation {} finished with no response body",
            op.name
        ))
    })?;
    let name = response
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            ConnectorError::Exchange(format!(
                "workspace events subscriptions.create operation {} response carries no subscription name",
                op.name
            ))
        })?
        .to_string();
    let expire_time = response
        .get("expireTime")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            ConnectorError::Exchange(format!(
                "workspace events subscriptions.create operation {} response carries no expireTime",
                op.name
            ))
        })?;
    let expires_at = DateTime::parse_from_rfc3339(expire_time)
        .map_err(|e| ConnectorError::Exchange(format!("subscription expireTime parse {expire_time:?}: {e}")))?
        .with_timezone(&Utc);
    Ok((name, expires_at))
}

/// Idempotently ensure a live Workspace Events subscription exists for this
/// member, (re)creating it if absent or imminently expiring.
///
/// **Called from TWO places** — `GoogleConnector::post_store` on a fresh
/// Chat connect, and every ingest pass ([`fetch_pending`], the sweep
/// worker). That dual call site is `google_gmail::ensure_watch`'s
/// self-healing shape verbatim (`g2-design.md:30`): a member who connected
/// before this PR, or whose subscription lapsed while the backend was down,
/// is repaired by ordinary traffic rather than by a one-off backfill script
/// somebody has to remember to run.
///
/// `control_pool` is ALWAYS the control-plane pool. `post_store`'s call site
/// already passes `routes::oauth::callback`'s own `state.db`, which is
/// control by construction. **Chat's watch rows inherit `#695`'s unsettled
/// plane split rather than resolving it** — design doc §G4.6 names that
/// settlement as G4.6's job explicitly ("Settle the single-plane decision
/// for Chat's watch rows here rather than inheriting the ambiguity"), and
/// pre-empting it here would settle a live-surface question for Gmail by
/// side effect. On the personal plane, which is the only plane Chat can
/// reach until G4.6, the two pools are the same database and the question
/// has no observable answer yet.
pub(crate) async fn ensure_subscription(
    http: &reqwest::Client,
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    sub: &str,
    access_token: &str,
) -> Result<(), ConnectorError> {
    if sub.trim().is_empty() {
        // Without a stable `sub` there is no upsert key (§1.2), and a row
        // keyed on an empty string would collide across every member of the
        // account. Refuse loudly rather than write a poisoned key — the
        // empty-placeholder lesson `0183`'s round-1 finding records.
        return Err(ConnectorError::Internal(
            "google chat: cannot ensure a subscription without the member's Google sub".into(),
        ));
    }
    if watch_is_live(control_pool, account_id, sub).await? {
        return Ok(());
    }
    let topic_name = std::env::var("GOOGLE_CHAT_PUBSUB_TOPIC")
        .map_err(|_| ConnectorError::NotConfigured("GOOGLE_CHAT_PUBSUB_TOPIC not configured".into()))?;

    let url = format!("{}/subscriptions", workspace_events_base());
    let body = subscription_request_body(&topic_name);
    let resp = google_send(|| http.post(super::google_transport::endpoint(&url)).bearer_auth(access_token).json(&body)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!(
            "chat subscriptions.create failed: {status}"
        )));
    }
    let op: crate::workers::google_watch_renewal::Operation = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("subscriptions.create response parse: {e}")))?;
    let op = crate::workers::google_watch_renewal::poll_operation(http, access_token, op).await?;
    let (subscription_name, expires_at) = subscription_from_operation(&op)?;

    upsert_watch(
        control_pool,
        account_id,
        owner_user_id,
        sub,
        &subscription_name,
        &topic_name,
        expires_at,
    )
    .await
}

/// Best-effort `subscriptions.delete` on disconnect (design doc §1.8). The
/// module `connectors::google_disconnect` deliberately *"makes no upstream
/// calls at all"* because no app token exists for Google in the delegated
/// case — Chat is the exception that proves the rule: the DISCONNECTING
/// MEMBER'S OWN token can revoke the member's own subscription, so we try,
/// and we say so in the PR body rather than letting that module's doc
/// quietly become false. Never fatal: the row is deleted locally either way,
/// and an orphaned upstream subscription simply expires within 7 days
/// (§4.2's TTL) delivering pushes we discard as unwatched (§4.3).
pub(crate) async fn delete_subscription_best_effort(
    http: &reqwest::Client,
    access_token: &str,
    subscription_name: &str,
) -> bool {
    if subscription_name.trim().is_empty() {
        return false;
    }
    let url = format!("{}/{subscription_name}", workspace_events_base());
    match google_send(|| http.delete(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await {
        Ok(resp) if resp.status().is_success() => {
            if resp.status() == reqwest::StatusCode::NO_CONTENT { return true; }
            let operation: crate::workers::google_watch_renewal::Operation = match resp.json().await {
                Ok(op) => op,
                Err(e) => { tracing::info!(error=%e,"Workspace subscription delete operation parse failed");return false; }
            };
            match crate::workers::google_watch_renewal::poll_operation(http,access_token,operation).await {
                Ok(op) => op.done && op.error.is_none(),
                Err(e) => { tracing::info!(error=%e,"Workspace subscription delete operation failed");false }
            }
        },
        Ok(resp) => {
            tracing::info!(
                subscription = subscription_name,
                status = %resp.status(),
                "google chat: best-effort subscriptions.delete on disconnect did not succeed — \
                 the subscription expires on its own within the 7-day TTL and its pushes are \
                 discarded as unwatched in the meantime"
            );
            false
        }
        Err(e) => {
            tracing::info!(
                subscription = subscription_name, error = %e,
                "google chat: best-effort subscriptions.delete on disconnect failed (non-fatal)"
            );
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Pagination decision — ported VERBATIM from `google_gmail::decide_after_page`
// (`google_gmail.rs:618`-`:641`), with its four boundary tests.
// ---------------------------------------------------------------------------

/// Whether accumulating one more page would push a pass past its per-call
/// `budget` — pure, identical shape to `google_gmail::budget_exceeded`.
fn budget_exceeded(accumulated: usize, budget: usize) -> bool {
    accumulated >= budget
}

enum PageDecision {
    Continue(String),
    Done {
        resume_page_token: Option<String>,
        /// `true` iff this pass reached the listing's TERMINAL page — the
        /// only condition under which a caller may bank a completed cursor.
        terminal: bool,
    },
}

/// What to do after accumulating one more page. **A TERMINAL page
/// (`next_page_token: None`) always finishes the pass and reports
/// `terminal: true`, regardless of budget** — this is #677's CRITICAL
/// finding, ported here for the third time (Calendar → Gmail → Chat)
/// because it recurs in every paginated Google sweep: checking the budget
/// FIRST would report a terminal page as "resume from None", which a
/// resume-from-`None` caller reads as "start over", looping forever on the
/// last page of any listing that happens to end exactly at the budget.
fn decide_after_page(next_page_token: Option<String>, accumulated: usize, budget: usize) -> PageDecision {
    match next_page_token.filter(|t| !t.is_empty()) {
        None => PageDecision::Done {
            resume_page_token: None,
            terminal: true,
        },
        Some(next) => {
            if budget_exceeded(accumulated, budget) {
                PageDecision::Done {
                    resume_page_token: Some(next),
                    terminal: false,
                }
            } else {
                PageDecision::Continue(next)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// §4.4 — per-SPACE cursor persistence (content plane)
// ---------------------------------------------------------------------------

/// The change-detection state for one watched space. Unlike Gmail (one
/// cursor per mailbox, on `google_watch_channels`), Chat's cursors are per
/// CONTAINER — one subscription covers every space, so the subscription row
/// has nothing per-space to say.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SpaceSyncState {
    /// The catch-up watermark (`spaces.spaceEvents.list`'s `startTime`).
    /// `None`/empty until the first completed catch-up pass; the BACKFILL
    /// is what runs before that (§4.4's "the two paths meet when
    /// `backfill_complete` flips").
    pub(crate) sync_token: Option<String>,
    /// `spaces.messages.list`'s resumable `pageToken`.
    pub(crate) backfill_cursor: Option<String>,
    /// `0198`'s column defaults TRUE ("a row with no backfill machinery
    /// reads as nothing-pending"), so [`pick_space_destination`]'s successful
    /// write is what ARMS a backfill by flipping it FALSE — see
    /// [`arm_backfill_for_space`].
    pub(crate) backfill_complete: bool,
}

/// `owner_user_id: None` reads the CONTAINER archetype's row
/// (`owner_user_id IS NULL`, G4.3/G4.4); `Some(uid)` reads G4.5's MAILBOX
/// archetype row for that specific opting-in member — `IS NOT DISTINCT FROM`
/// so the same predicate serves both without a second query. See this
/// module's doc for why the two archetypes can safely share one set of
/// cursor-plumbing functions: the row's key is `(account_id, space_id,
/// owner_user_id)` either way, and everything downstream of the cursor
/// (backfill/catch-up paging, message normalisation, atom writes) is already
/// archetype-agnostic.
pub(crate) async fn load_space_sync_state(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Option<Uuid>,
) -> Result<Option<SpaceSyncState>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "load_space_sync_state").await?;
    let row: Option<(Option<String>, Option<String>, bool)> = sqlx::query_as(
        "SELECT sync_token, backfill_cursor, backfill_complete FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NOT DISTINCT FROM $3",
    )
    .bind(account_id)
    .bind(space_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("load_space_sync_state: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(sync_token, backfill_cursor, backfill_complete)| SpaceSyncState {
        sync_token,
        backfill_cursor,
        backfill_complete,
    }))
}

/// Update the cursor columns on an EXISTING watched-space row. Never
/// inserts — the row is created by `ADDED_TO_SPACE` (G4.3) and this function
/// is only ever reached for a space that already resolved a destination.
///
/// `sync_token: None` PRESERVES the stored value (a mid-pass, backfill-only
/// write must not clear a catch-up watermark); `Some` overwrites it.
#[allow(clippy::too_many_arguments)]
async fn upsert_space_sync_columns(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Option<Uuid>,
    sync_token: Option<&str>,
    backfill_cursor: Option<&str>,
    backfill_complete: bool,
    bump_last_ingested: bool,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "upsert_space_sync_columns").await?;
    sqlx::query(
        "UPDATE google_chat_watched_spaces \
         SET sync_token = COALESCE($4, sync_token), \
             backfill_cursor = $5, \
             backfill_complete = $6, \
             last_ingested_at = CASE WHEN $7 THEN now() ELSE last_ingested_at END \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NOT DISTINCT FROM $3",
    )
    .bind(account_id)
    .bind(space_id)
    .bind(owner_user_id)
    .bind(sync_token)
    .bind(backfill_cursor)
    .bind(backfill_complete)
    .bind(bump_last_ingested)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("upsert_space_sync_columns: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("upsert_space_sync_columns commit: {e}")))?;
    Ok(())
}

/// Arm a freshly-routed space's backfill: `backfill_complete = FALSE`,
/// cursor cleared. `0198` defaults the column TRUE (matching
/// `gmail_watched_threads`' "nothing pending" convention), so without this
/// an unrouted-then-routed space would never backfill at all — it would
/// only ever see messages arriving AFTER the pick, silently breaking §5.4's
/// promise that auto-seated members read the ocean back to its start.
///
/// Idempotent and NON-destructive: it refuses to re-arm a space that has
/// already ingested anything (`last_ingested_at IS NOT NULL`), so a second
/// call — a duplicate route action, a G4-Q6 re-add-then-re-pick — never
/// restarts a completed backfill from scratch.
pub(crate) async fn arm_backfill_for_space(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Option<Uuid>,
) -> Result<bool, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "arm_backfill_for_space").await?;
    let armed: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE google_chat_watched_spaces \
         SET backfill_complete = false, backfill_cursor = NULL \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NOT DISTINCT FROM $3 \
           AND last_ingested_at IS NULL AND backfill_complete = true \
         RETURNING id",
    )
    .bind(account_id)
    .bind(space_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("arm_backfill_for_space: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("arm_backfill_for_space commit: {e}")))?;
    Ok(armed.is_some())
}

// ---------------------------------------------------------------------------
// §4.4 — backfill (`spaces.messages.list`, orderBy createTime ASC)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct MessagesListResponse {
    #[serde(default)]
    messages: Vec<ChatMessage>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// Bounds travel with the opaque provider token across sweep continuations.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct HistoryCursor {
    since: chrono::DateTime<Utc>,
    until: chrono::DateTime<Utc>,
    page: Option<String>,
}
impl HistoryCursor {
    pub(crate) fn new(until: chrono::DateTime<Utc>, days: u32) -> Self {
        Self { since: until - chrono::Duration::days(days.min(28) as i64), until, page: None }
    }
    pub(crate) fn encode(&self) -> String { format!("history-v1:{}", serde_json::to_string(self).expect("history cursor")) }
    fn contains(&self, time: Option<&str>) -> bool {
        time.and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
            .is_some_and(|v| v >= self.since && v <= self.until)
    }
}

async fn list_messages_page(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
    page_token: Option<&str>,
    bounds: Option<&HistoryCursor>,
) -> Result<MessagesListResponse, ConnectorError> {
    // `orderBy=createTime ASC` — Chat's own default, stated explicitly
    // because it is the resumable direction (§2.2) and the opposite of
    // Meet's descending default; a silent upstream default flip would turn
    // a resumable forward walk into a newest-first one that never converges.
    let mut url = format!(
        "{}/{space_id}/messages?pageSize={MESSAGES_PAGE_SIZE}&orderBy={}",
        chat_api_base(),
        urlencoding::encode("createTime ASC")
    );
    if let Some(pt) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&urlencoding::encode(pt));
    }
    if let Some(b) = bounds {
        url.push_str("&filter=");
        url.push_str(&urlencoding::encode(&format!("createTime >= \"{}\" AND createTime <= \"{}\"", b.since.to_rfc3339(), b.until.to_rfc3339())));
    }
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!(
            "chat spaces.messages.list failed: {status}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("chat spaces.messages.list parse: {e}")))
}

/// One bounded backfill pass over a space's history. `items` are shaped but
/// NOT persisted, and `resume_cursor`/`complete` are NOT written — the
/// caller ingests first and only then commits, the same commit-after-ingest
/// split [`fetch_pending`]/[`commit_pending`] enforce for the live path.
pub(crate) struct BackfillPass {
    pub(crate) items: Vec<NormalizedItem>,
    pub(crate) resume_cursor: Option<String>,
    pub(crate) complete: bool,
    /// Messages the funnel filtered as poison pills (§3.7 rule 7) — counted,
    /// never silently dropped.
    pub(crate) filtered: usize,
}

/// Walk `spaces.messages.list` from `resume_cursor` up to [`SYNC_PASS_BUDGET`].
pub(crate) async fn fetch_backfill_pass(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
    resume_cursor: Option<String>,
    own_bot_user_id: &str,
    directory: &HashMap<String, String>,
) -> Result<BackfillPass, ConnectorError> {
    let mut bounds = resume_cursor.as_deref().and_then(|s| s.strip_prefix("history-v1:"))
        .map(serde_json::from_str::<HistoryCursor>).transpose()
        .map_err(|e| ConnectorError::Internal(format!("invalid history cursor: {e}")))?;
    let mut page_token = bounds.as_ref().and_then(|b| b.page.clone()).or_else(|| if bounds.is_none() { resume_cursor } else { None });
    let mut items: Vec<NormalizedItem> = Vec::new();
    let mut filtered = 0usize;
    let mut seen: HashSet<String> = HashSet::new();
    loop {
        let page = list_messages_page(http, access_token, space_id, page_token.as_deref(), bounds.as_ref()).await?;
        let next_page_token = page.next_page_token.clone();
        for m in &page.messages {
            if bounds.as_ref().is_some_and(|b| !b.contains(m.create_time.as_deref())) {
                filtered += 1;
                continue;
            }
            if !seen.insert(m.name.trim().to_string()) {
                continue;
            }
            match chat_message_to_item(m, own_bot_user_id, &|id: &str| directory.get(id).cloned()) {
                Some(item) => items.push(item),
                None => filtered += 1,
            }
        }
        match decide_after_page(next_page_token, items.len() + filtered, SYNC_PASS_BUDGET) {
            PageDecision::Continue(next) => page_token = Some(next),
            PageDecision::Done {
                resume_page_token,
                terminal,
            } => {
                return Ok(BackfillPass {
                    items,
                    resume_cursor: if terminal { None } else if let Some(b) = bounds.as_mut() {
                        b.page = resume_page_token;
                        Some(b.encode())
                    } else { resume_page_token },
                    complete: terminal,
                    filtered,
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------
// §4.4/§2.7 — catch-up (`spaces.spaceEvents.list`)
// ---------------------------------------------------------------------------

/// A `SpaceEvent`, decoded defensively.
///
/// **The per-event-type body shape is UNVERIFIED** (design doc §2.7, risk
/// 14; `tests/fixtures/google_chat/space_events_list.json` is hand-authored
/// against Google's general Workspace Events shape, not captured, and its
/// README says so). Every field below is `#[serde(default)]` and every
/// consumer reads at most a resource `name` out of it, so a shape surprise
/// degrades to "this event yielded no message id" — logged, counted,
/// skipped — rather than a parse failure that fails a whole pass, and
/// certainly not a panic. The batch variants are handled because Google
/// delivers them automatically for any subscribed type without being asked
/// (§2.7: *"Omit batch event types in your filter. The request
/// automatically returns any related batch events."*).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpaceEvent {
    #[serde(default)]
    event_type: String,
    #[serde(default)]
    message_created_event_data: Option<MessageEventData>,
    #[serde(default)]
    message_deleted_event_data: Option<MessageEventData>,
    #[serde(default)]
    message_batch_created_event_data: Option<MessageBatchEventData>,
    #[serde(default)]
    message_batch_deleted_event_data: Option<MessageBatchEventData>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct MessageEventData {
    #[serde(default)]
    message: Option<MessageRef>,
}

#[derive(Debug, Default, Deserialize)]
struct MessageBatchEventData {
    #[serde(default)]
    messages: Vec<MessageEventData>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct MessageRef {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
struct SpaceEventsListResponse {
    #[serde(default, rename = "spaceEvents")]
    space_events: Vec<SpaceEvent>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// Split one `spaceEvents.list` page into `(created message names, deleted
/// message names)`. Pure — no I/O — so the UNVERIFIED payload shape is
/// directly testable against the fixture without a live API.
///
/// **Only resource NAMES are taken, never content** — contract §4's C7 row
/// ("treat pushes as triggers, not as data"), applied to the catch-up path
/// too. `spaceEvents.list` may well return a full `Message` inline, but
/// trusting it would make the ingest funnel's behaviour depend on a payload
/// shape nobody has verified; re-fetching through `spaces.messages.get`
/// costs one round trip and makes the whole catch-up path shape-agnostic.
/// A deleted message's name is enough on its own — it IS the `source_id`
/// (§3.6), verbatim, with no id reconstruction.
fn process_space_events_page(page: SpaceEventsListResponse) -> (Vec<String>, Vec<String>) {
    fn push_name(out: &mut Vec<String>, data: &Option<MessageEventData>) {
        if let Some(m) = data.as_ref().and_then(|d| d.message.as_ref()) {
            let name = m.name.trim();
            if name.is_empty() {
                tracing::warn!("google_chat: spaceEvents entry with a missing/empty message name skipped");
                return;
            }
            out.push(name.to_string());
        }
    }
    fn push_batch(out: &mut Vec<String>, data: &Option<MessageBatchEventData>) {
        let Some(batch) = data.as_ref() else { return };
        for entry in &batch.messages {
            push_name(out, &Some(entry.clone()));
        }
    }

    let mut created = Vec::new();
    let mut deleted = Vec::new();
    for ev in &page.space_events {
        // Read the typed payload first; fall back to `eventType` only as a
        // tiebreak, so an event whose body arrives in an unexpected wrapper
        // is skipped-and-counted rather than mis-classified as the other
        // kind (a created message routed into the deletion path would
        // delete content that was just posted).
        if ev.message_created_event_data.is_some() {
            push_name(&mut created, &ev.message_created_event_data);
        }
        if ev.message_batch_created_event_data.is_some() {
            push_batch(&mut created, &ev.message_batch_created_event_data);
        }
        if ev.message_deleted_event_data.is_some() {
            push_name(&mut deleted, &ev.message_deleted_event_data);
        }
        if ev.message_batch_deleted_event_data.is_some() {
            push_batch(&mut deleted, &ev.message_batch_deleted_event_data);
        }
        if ev.message_created_event_data.is_none()
            && ev.message_deleted_event_data.is_none()
            && ev.message_batch_created_event_data.is_none()
            && ev.message_batch_deleted_event_data.is_none()
        {
            tracing::debug!(
                event_type = %ev.event_type,
                "google_chat: spaceEvents entry carried no message payload this connector \
                 consumes — skipped (membership/reaction/space events, or an unrecognised shape)"
            );
        }
    }
    (created, deleted)
}

/// The mandatory `filter` for a message-only catch-up (§2.7: `filter` is
/// required, must use the has-operator, `OR` between event types, `AND` for
/// the time bound, and must NOT name batch types). Built by a named function
/// so its exact shape is testable.
fn space_events_filter(start_time: Option<&str>) -> String {
    let base = "eventTypes:\"google.workspace.chat.message.v1.created\" OR \
                eventTypes:\"google.workspace.chat.message.v1.deleted\"";
    match start_time {
        Some(ts) => format!("({base}) AND startTime=\"{ts}\""),
        None => base.to_string(),
    }
}

/// Floor a stored watermark at `now - 28 days` (§2.7's retention bound).
/// Returns the RFC-3339 string to send upstream. Pure.
fn floored_watermark(stored: Option<&str>, now: DateTime<Utc>) -> String {
    let floor = now - chrono::Duration::days(SPACE_EVENTS_RETENTION_DAYS);
    let parsed = stored
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));
    match parsed {
        Some(ts) if ts > floor => ts.to_rfc3339(),
        _ => floor.to_rfc3339(),
    }
}

async fn list_space_events_page(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
    filter: &str,
    page_token: Option<&str>,
) -> Result<SpaceEventsListResponse, ConnectorError> {
    let mut url = format!(
        "{}/{space_id}/spaceEvents?pageSize={SPACE_EVENTS_PAGE_SIZE}&filter={}",
        chat_api_base(),
        urlencoding::encode(filter)
    );
    if let Some(pt) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&urlencoding::encode(pt));
    }
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!(
            "chat spaces.spaceEvents.list failed: {status}"
        )));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("chat spaces.spaceEvents.list parse: {e}")))
}

/// The outcome of one authoritative message re-read.
pub(crate) enum MessageFetch {
    Found(Box<ChatMessage>),
    /// `404`/`403`/`410` — this specific message will never be readable
    /// again with this token: it was deleted after the event was emitted, or
    /// the reader lost access to it. **Terminal for this message, and
    /// therefore CONSUMED** — retrying it forever is what wedges a space.
    Gone,
}

/// Whether an HTTP status means "this message is permanently unreadable"
/// rather than "try again". Pure, so the classification is testable without
/// a live API.
///
/// `404` (deleted / never existed), `403` (the reader lost access — note
/// this is per-READER, and the sweep's own failover re-election is what
/// covers the case where a DIFFERENT member could still read it) and `410`
/// (gone) are permanent. Everything else — `429`, every `5xx`, a network
/// error, a parse failure — is transient and must block the watermark.
fn message_fetch_is_permanent(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::GONE
    )
}

/// `GET /v1/{name=spaces/*/messages/*}` — the authoritative re-read every
/// trigger-driven path goes through (§4.3).
pub(crate) async fn get_message(
    http: &reqwest::Client,
    access_token: &str,
    message_name: &str,
) -> Result<MessageFetch, ConnectorError> {
    let url = format!("{}/{message_name}", chat_api_base());
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        if message_fetch_is_permanent(status) {
            return Ok(MessageFetch::Gone);
        }
        return Err(ConnectorError::upstream(status, format!(
            "chat spaces.messages.get failed: {status}"
        )));
    }
    resp.json()
        .await
        .map(|m: ChatMessage| MessageFetch::Found(Box::new(m)))
        .map_err(|e| ConnectorError::Exchange(format!("chat spaces.messages.get parse: {e}")))
}

// ---------------------------------------------------------------------------
// §4.5 — the pending/commit split (the unit of work is ONE MESSAGE, PER SPACE)
// ---------------------------------------------------------------------------

/// What [`fetch_pending`] found, deferred rather than persisted — see
/// `google_gmail::PendingSync`'s doc for the commit-after-ingest rationale
/// this mirrors.
pub(crate) struct PendingSync {
    pub(crate) items: Vec<NormalizedItem>,
    pub(crate) commit: PendingCommit,
    pub(crate) filtered: usize,
}

pub(crate) enum PendingCommit {
    /// A backfill pass: `(resume cursor, reached the terminal page)`.
    ///
    /// `seed_watermark` is `Some(pass_started_at)` **only** on the pass that
    /// COMPLETES the backfill, and it is the fix for a convergence bug found
    /// in review round 1: `sync_token` starts NULL, `commit_pending`'s
    /// backfill arm passes `sync_token: None`, `upsert_space_sync_columns`
    /// does `COALESCE(NULL, sync_token)` — so the watermark was still NULL
    /// the moment the backfill finished, and [`floored_watermark`] then
    /// floored it to `now - 28 days`. EVERY space re-walked a full 28 days of
    /// `spaceEvents` immediately after its backfill had already ingested
    /// exactly that content. Seeding the watermark at handoff makes the
    /// backfill→catch-up transition continuous.
    ///
    /// `pass_started_at`, not `now()`: `orderBy=createTime ASC` means a
    /// message posted DURING the backfill sorts after the current cursor and
    /// is therefore still reached by the forward walk, but stamping the
    /// earlier instant makes the two windows OVERLAP rather than risk a gap.
    /// An overlap is free — every write is `ON CONFLICT DO NOTHING`.
    Backfill {
        resume_cursor: Option<String>,
        complete: bool,
        seed_watermark: Option<String>,
    },
    /// A catch-up pass that reached its terminal page — bank the new
    /// watermark.
    CatchUpComplete(String),
    /// A catch-up pass that stopped before the terminal page (budget, or a
    /// transient per-message failure). **The watermark deliberately does NOT
    /// advance** — `spaceEvents.list` is re-listable within its 28-day window
    /// and every write is idempotent, whereas advancing past un-ingested
    /// events would lose them permanently.
    ///
    /// `resume_cursor` DOES advance, and that is the other half of review
    /// round 1's convergence fix: without it a space with more than
    /// [`SYNC_PASS_BUDGET`] events in its trailing 28 days re-listed the
    /// identical window every tick, forever, at ~2020 API calls a tick and
    /// zero forward progress — anything busier than ~72 messages a day never
    /// converged. It is stored in `backfill_cursor`, whose column is free
    /// once `backfill_complete` is true; the overload is deliberate and is
    /// exactly the one `google_gmail`'s own module doc documents for
    /// `backfill_page_token` ("same columns, same 'resume an in-flight
    /// paginated fetch' meaning, applied to a diff-only API instead of a
    /// full-listing one"). The filter is byte-identical across ticks because
    /// the watermark it is built from has not moved, which is what makes an
    /// opaque `pageToken` safe to carry (§2.2: "passing different values to
    /// the other parameters might lead to unexpected results").
    CatchUpProgress { resume_cursor: Option<String> },
    /// Nothing to persist — the ONLY arm that issues no statement at all.
    ///
    /// Two distinct callers: the benign ones (no destination, unwatched,
    /// nothing found) and the *retryable-failure* one (deletion events that
    /// could not even be attempted, see [`fetch_pending`]). For the second,
    /// "issues no statement" is a correctness requirement, not an
    /// optimisation: every other arm reaches `upsert_space_sync_columns`,
    /// which writes `backfill_cursor` unconditionally and can stamp
    /// `last_ingested_at`. Do not "simplify" a retryable path onto
    /// `CatchUpProgress { resume_cursor: None }` — that is a commit.
    Nothing,
}

/// The commit value for a pass whose deletions could not even be ATTEMPTED.
///
/// A named function, not an inline literal, purely so the property that
/// matters — *this path writes nothing at all* — is pinnable by a test.
/// The branch itself is only reachable behind a live `spaceEvents.list`
/// round trip against `chat.googleapis.com`, which no test here can drive
/// (there is no API-base override and no HTTP mock in this crate), so
/// `un_propagatable_deletion_commit_writes_nothing_at_all_pg` asserts on
/// this function's value instead: it feeds it to [`commit_pending`] against
/// a real row carrying a watermark, a resume cursor and a `last_ingested_at`,
/// and requires every column to come back byte-identical. Reverting the
/// branch to `CatchUpProgress { resume_cursor: None }` therefore fails a
/// test rather than silently resuming the data loss.
fn un_propagatable_deletion_commit() -> PendingCommit {
    PendingCommit::Nothing
}

/// `owner_user_id`: see [`load_space_sync_state`]'s doc — `None` for the
/// container archetype, `Some(uid)` for G4.5's mailbox opt-in.
pub(crate) async fn commit_pending(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Option<Uuid>,
    commit: PendingCommit,
) -> Result<(), ConnectorError> {
    match commit {
        PendingCommit::Backfill {
            resume_cursor,
            complete,
            seed_watermark,
        } => {
            upsert_space_sync_columns(
                content_pool,
                account_id,
                space_id,
                owner_user_id,
                seed_watermark.as_deref(),
                resume_cursor.as_deref(),
                complete,
                true,
            )
            .await
        }
        PendingCommit::CatchUpComplete(watermark) => {
            upsert_space_sync_columns(content_pool, account_id, space_id, owner_user_id, Some(&watermark), None, true, true).await
        }
        // The watermark is preserved (`sync_token: None` ⇒ `COALESCE`), the
        // resume cursor advances, and `backfill_complete` stays TRUE so the
        // next pass re-enters the CATCH-UP path rather than restarting a
        // backfill.
        PendingCommit::CatchUpProgress { resume_cursor } => {
            upsert_space_sync_columns(
                content_pool,
                account_id,
                space_id,
                owner_user_id,
                None,
                resume_cursor.as_deref(),
                true,
                true,
            )
            .await
        }
        PendingCommit::Nothing => Ok(()),
    }
}

/// Fetch one pass for `space_id` WITHOUT persisting anything — backfill
/// while `backfill_complete` is false, catch-up afterwards (§4.4: "the two
/// paths meet when `backfill_complete` flips and the watermark takes over").
///
/// Deletions are applied EAGERLY inside this call (idempotent, safe to
/// retry) and a propagation failure propagates via `?`, leaving the cursor
/// unchanged — #686 round 2's finding, and non-negotiable for Chat because
/// `spaceEvents.list` reports a given deletion exactly once per range.
/// `owner_user_id`: see [`load_space_sync_state`]'s doc — `None` for the
/// container archetype (G4.3/G4.4), `Some(uid)` for G4.5's mailbox opt-in.
/// Everything below this line — backfill paging, catch-up paging, deletion
/// propagation, message re-fetch — is unchanged by which archetype called
/// it; only the row this reads/advances differs.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_pending(
    http: &reqwest::Client,
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Option<Uuid>,
    access_token: &str,
    own_bot_user_id: &str,
    directory: &HashMap<String, String>,
    deletion_anchor_graph_id: Option<Uuid>,
) -> Result<PendingSync, ConnectorError> {
    let pass_started_at = Utc::now();
    let Some(state) = load_space_sync_state(content_pool, account_id, space_id, owner_user_id).await? else {
        // No watched row at all — the ADR-0008 storage-scoping discard
        // (§4.3). The member's `spaces/-` subscription deliberately reaches
        // wider than our consent set; this filter is what reconciles them.
        return Ok(PendingSync {
            items: Vec::new(),
            commit: PendingCommit::Nothing,
            filtered: 0,
        });
    };

    if !state.backfill_complete {
        let pass = fetch_backfill_pass(
            http,
            access_token,
            space_id,
            state.backfill_cursor.clone(),
            own_bot_user_id,
            directory,
        )
        .await?;
        // Hand the catch-up path a watermark the moment the backfill
        // finishes — see `PendingCommit::Backfill::seed_watermark`'s doc for
        // the 28-day re-walk this closes.
        let seed_watermark = pass.complete.then(|| {
            state.backfill_cursor.as_deref().and_then(|s| s.strip_prefix("history-v1:"))
                .and_then(|s| serde_json::from_str::<HistoryCursor>(s).ok())
                .map(|b| b.until).unwrap_or(pass_started_at).to_rfc3339()
        });
        return Ok(PendingSync {
            items: pass.items,
            commit: PendingCommit::Backfill {
                resume_cursor: pass.resume_cursor,
                complete: pass.complete,
                seed_watermark,
            },
            filtered: pass.filtered,
        });
    }

    let watermark = floored_watermark(state.sync_token.as_deref(), pass_started_at);
    let filter = space_events_filter(Some(&watermark));

    // Resume an interrupted catch-up exactly where it stopped. The filter is
    // byte-identical to the one that minted this token (the watermark has
    // not moved — `CatchUpProgress` deliberately does not advance it), which
    // is what makes carrying an opaque `pageToken` across ticks safe.
    let mut page_token: Option<String> = state.backfill_cursor.clone();
    let mut created: Vec<String> = Vec::new();
    let mut deleted: Vec<String> = Vec::new();
    let terminal;
    let resume_cursor: Option<String>;
    loop {
        let page = list_space_events_page(http, access_token, space_id, &filter, page_token.as_deref()).await?;
        let next_page_token = page.next_page_token.clone();
        let (mut page_created, mut page_deleted) = process_space_events_page(page);
        created.append(&mut page_created);
        deleted.append(&mut page_deleted);
        match decide_after_page(next_page_token, created.len() + deleted.len(), SYNC_PASS_BUDGET) {
            PageDecision::Continue(next) => page_token = Some(next),
            PageDecision::Done {
                resume_page_token,
                terminal: t,
            } => {
                terminal = t;
                resume_cursor = resume_page_token;
                break;
            }
        }
    }

    // A message CREATED and then DELETED inside the same event range is a
    // deletion, full stop (review round 1, H2): post-then-delete a typo and
    // the same resource name lands in BOTH lists. Re-fetching it is a
    // guaranteed 404, which — before this — Err'd the whole pass, left the
    // watermark unmoved, and repeated every tick for up to 28 days. It is
    // also simply wrong to ingest something we already know is gone.
    let deleted_set: HashSet<&str> = deleted.iter().map(String::as_str).collect();
    let created: Vec<String> = created
        .into_iter()
        .filter(|name| !deleted_set.contains(name.as_str()))
        .collect();

    // Deletions first, eagerly, and blocking the cursor on failure.
    if !deleted.is_empty() {
        match deletion_anchor_graph_id {
            Some(anchor) => propagate_deletions(content_pool, account_id, anchor, &deleted).await?,
            // Review round 1, M3: this used to `debug!` "nothing was ever
            // ingested for it" and fall through to a COMMIT — unsupportable
            // for a space that has atoms and merely resolved zero
            // destinations this pass (an org fan-out with no mappable
            // members). With no creations in range the pass then committed
            // the watermark and those deletions were lost permanently: a C9
            // violation with no operator signal at all. A deletion we cannot
            // even attempt is RETRYABLE, so nothing is committed.
            //
            // `PendingCommit::Nothing`, NOT `CatchUpProgress { resume_cursor:
            // None }` (review round 2). That earlier value was described in
            // this very comment as "nothing is committed" while committing
            // twice over: `commit_pending` routes it into
            // `upsert_space_sync_columns(.., backfill_cursor = $4, ..,
            // bump_last_ingested = true)`, whose SQL sets `backfill_cursor`
            // UNCONDITIONALLY — so a `None` cursor ERASED whatever catch-up
            // resume cursor was stored — and stamps
            // `last_ingested_at = now()`, the freshness signal an operator
            // reads to decide whether a space is healthy. The net effect was
            // the exact pair of harms the M3 fix existed to prevent: a space
            // that could not honour its deletions lost its place in the
            // page walk AND reported itself as freshly ingested. `Nothing`
            // is the only arm of `commit_pending` that issues no statement at
            // all, so it is the only one that makes this comment true.
            None => {
                tracing::warn!(
                    %account_id, space_id, deletions = deleted.len(),
                    connector_health = true,
                    "google_chat: deletion events arrived for a space that resolved NO destination \
                     ocean this pass, so they could not be propagated. The watermark is NOT \
                     advanced and the next pass retries — but until a destination resolves, \
                     upstream deletions are not being honoured for this space."
                );
                return Ok(PendingSync {
                    items: Vec::new(),
                    commit: un_propagatable_deletion_commit(),
                    filtered: 0,
                });
            }
        }
    }

    // Re-fetch every created message authoritatively (§4.3, C7).
    //
    // **One bad message must not discard the rest of an already-walked
    // range** (review round 1, H2) — the same rule #722 had to add to Gmail's
    // everything-mode loop (`google_gmail.rs:1423`-`:1442`). A PERMANENT
    // failure (404/403/410) is consumed and counted; a TRANSIENT one is
    // logged, the rest of the range still proceeds, and the pass downgrades
    // to `CatchUpProgress` so the watermark does not advance past it. That
    // downgrade is the analogue of Gmail's 422-is-consumed rule, inverted for
    // Path 1: there is no `ingest_atom` status here, so the funnel's own
    // permanent/transient split carries it.
    let mut items = Vec::new();
    let mut filtered = 0usize;
    let mut transient_failures = 0usize;
    let mut seen: HashSet<String> = HashSet::new();
    for name in &created {
        if !seen.insert(name.clone()) {
            continue;
        }
        match get_message(http, access_token, name).await {
            Ok(MessageFetch::Found(msg)) => {
                match chat_message_to_item(&msg, own_bot_user_id, &|id: &str| directory.get(id).cloned()) {
                    Some(item) => items.push(item),
                    None => filtered += 1,
                }
            }
            Ok(MessageFetch::Gone) => {
                filtered += 1;
                tracing::info!(
                    %account_id, space_id, source_id = %name,
                    "google_chat: message is permanently unreadable (404/403/410) — consumed, \
                     never blocking the watermark"
                );
            }
            Err(e) => {
                transient_failures += 1;
                tracing::warn!(
                    error = %e, %account_id, space_id, source_id = %name,
                    "google_chat: spaces.messages.get failed transiently for one message — the \
                     rest of this range still proceeds, and the watermark will NOT advance past it"
                );
            }
        }
    }

    // A transient per-message failure blocks the watermark exactly as a
    // failed ingest does, but WITHOUT throwing away the rest of the range.
    let terminal = terminal && transient_failures == 0;

    Ok(PendingSync {
        items,
        commit: if terminal {
            PendingCommit::CatchUpComplete(pass_started_at.to_rfc3339())
        } else {
            PendingCommit::CatchUpProgress { resume_cursor }
        },
        filtered,
    })
}

// ---------------------------------------------------------------------------
// §5.3 — C9: deletion propagation
// ---------------------------------------------------------------------------

/// Propagate every deleted message name to
/// `services::deletion::delete_source`. Returns `Result`, and the caller
/// propagates via `?` so a failure leaves the cursor unchanged — #686 round
/// 2's finding, ported to Calendar in #690, and load-bearing here because
/// Chat is C9 = yes and a deletion is reported exactly once per event range.
/// A batch-lookup failure aborts immediately (nothing in the batch could be
/// resolved); an individual failure is logged, the rest of the batch is
/// still attempted, and the function returns `Err` at the end regardless.
///
/// # Deviation from design §3.6, discovered during implementation
///
/// The design says Chat's C9 is *"an exact-match `delete_source` with no id
/// reconstruction"* because `source_id` IS the string Google hands back.
/// The first half is right; the second is not, and the difference is a
/// silent no-op rather than a compile error. **`delete_source` matches
/// `atoms.source_id` EXACTLY** (`services/deletion.rs:1416`:
/// `a.source_id = $3`), while EVERY ingest path in this tree — this module's
/// own [`ingest_space_items`], `routes::ingest::ingest_atom`, and
/// `routes::slack_events::ingest_slack_message` alike — stores chunked atoms
/// under `"{source_id}:chunk:{position}"`. Handing `delete_source` the bare
/// message name therefore matches zero rows.
///
/// So this function resolves the CONCRETE `source_id`s first — the bare name
/// (the degenerate single-chunk case some other writer might produce) plus
/// every `"{name}:chunk:%"` sibling, escaped through the shared
/// `services::deletion::escaped_chunk_pattern` so a literal `%`/`_`/`\` in a
/// message name cannot act as a wildcard (that helper's own doc records the
/// HIGH finding that produced it) — and calls `delete_source` once per
/// resolved id. This is one extra bounded read per deletion batch, and it is
/// the difference between C9 working and C9 appearing to work.
///
/// **The same gap exists today for Gmail and Calendar** (`google_gmail::
/// propagate_deletions`, `google_calendar::propagate_deletions` both pass a
/// bare provider id), and their pg tests do not catch it because they seed
/// atoms with an un-chunked `source_id` rather than through the real ingest
/// shape — #677's fixture-bug class exactly. **Deliberately NOT fixed here**:
/// it is a live-surface behaviour change on two other connectors, out of
/// this PR's scope, and it is called out in the PR body instead of being
/// quietly swept in.
pub(crate) async fn propagate_deletions(
    content_pool: &PgPool,
    account_id: Uuid,
    anchor_graph_id: Uuid,
    deleted_message_names: &[String],
) -> Result<(), ConnectorError> {
    if deleted_message_names.is_empty() {
        return Ok(());
    }
    let mut any_failed = false;
    for source_id in deleted_message_names {
        let Some((space,_))=source_id.split_once("/messages/") else {
            return Err(ConnectorError::Internal("google_chat deletion has no space identity".into()));
        };
        let namespace=format!("google_chat:space:{space}");
        match crate::services::deletion::delete_chunked_source_in_namespace(
            content_pool,
            None,
            crate::store::CLOUD_DEVICE_ID,
            anchor_graph_id,
            SOURCE_TYPE,
            source_id,
            &namespace,
            // Push/sweep-driven propagation, no CoralSwarm user session —
            // same actor and same `control_pool: None` (FM-5) posture as
            // `google_gmail::propagate_deletions`' identical call site.
            crate::services::connector_audit::Actor::System,
            None,
        )
        .await
        {
            Ok(outcome) => {
                tracing::info!(%account_id, %source_id, ?outcome, "google_chat: message deletion propagated")
            }
            Err(e) => {
                tracing::warn!(error = %e, %account_id, %source_id, "google_chat: message deletion propagation failed");
                any_failed = true;
            }
        }
    }
    if any_failed {
        return Err(ConnectorError::Internal(format!(
            "google_chat: one or more deletions failed to propagate for account {account_id}"
        )));
    }
    Ok(())
}

/// Every `atoms.source_id` in this account that belongs to one of
/// `message_names` — the bare name plus its `":chunk:N"` siblings. See
/// [`propagate_deletions`]' doc for why this exists at all.
async fn concrete_source_ids(
    content_pool: &PgPool,
    account_id: Uuid,
    message_names: &[String],
) -> Result<Vec<String>, ConnectorError> {
    let patterns: Vec<String> = message_names
        .iter()
        .map(|n| crate::services::deletion::escaped_chunk_pattern(n))
        .collect();
    let mut tx = scoped_tx(content_pool, account_id, "concrete_source_ids").await?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT a.source_id FROM atoms a \
         JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND a.source_type = $2 \
           AND (a.source_id = ANY($3) OR a.source_id LIKE ANY($4))",
    )
    .bind(account_id)
    .bind(SOURCE_TYPE)
    .bind(message_names)
    .bind(&patterns)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("concrete_source_ids: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(s,)| s).collect())
}

// ---------------------------------------------------------------------------
// §3.4 / D7 — roster auto-seating
// ---------------------------------------------------------------------------

/// One `JOINED`, non-bot, non-group roster member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RosterMember {
    /// The bare Google `sub` (`User.name` with `"users/"` stripped) — the
    /// ONLY stable identifier the CHAT API ITSELF returns (§2.4: no email
    /// address is ever returned, on any resource, under any scope of
    /// `chat.googleapis.com`). **Still true** — what changed (chat-per-user-
    /// setup PR, 2026-09-20) is that this SAME `sub` doubles as the People
    /// API's `Person.resourceName` id, so it can be looked up in the
    /// Workspace's own domain directory (see this file's "Domain directory"
    /// section, below [`resolve_space_roster`]) for a verified email Chat's
    /// own API will still never hand back directly.
    pub(crate) sub: String,
    /// Possibly empty — see [`ChatUserRef::display_name`]'s UNVERIFIED note.
    /// The invite/seat path ignores this for an external member (item 43).
    pub(crate) display_name: String,
    /// `User.name` as returned (`users/{id}`). External members are persisted
    /// under this key, not the stripped [`Self::sub`].
    pub(crate) resource_name: String,
    /// `User.type` (`HUMAN`, or empty when the field was absent).
    pub(crate) user_type: String,
    /// `Membership.affiliation`. Empty when the field was absent.
    pub(crate) affiliation: String,
}

/// A space's roster, counted by Slack's rules verbatim (§3.4).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SpaceRoster {
    /// `M` — roster members minus bots minus non-`JOINED` minus groups.
    pub(crate) members: Vec<RosterMember>,
    /// Google Groups in the roster (§3.4a). Counted SEPARATELY from
    /// `unmapped`: *"we cannot enumerate these at all"* is a different
    /// statement from *"these people have no CoralSwarm account"*, and
    /// founder answer Q3 is **allow + disclose**, not refuse. A group-bearing
    /// space is still eligible for a shared destination — contract §4's C3
    /// row is about being unable to enumerate the roster AT ALL, not part of
    /// it (§3.4a).
    pub(crate) group_memberships: usize,
    /// `member.type == BOT` entries, excluded outright.
    pub(crate) bots_excluded: usize,
    /// `state != JOINED` (an `INVITED` member has not accepted and is not
    /// yet part of the audience — §2.4).
    pub(crate) not_joined_excluded: usize,
}

impl SpaceRoster {
    pub(crate) fn total_humans(&self) -> usize {
        self.members.len()
    }
}

/// Apply §3.4's counting rules to a raw roster. Pure — directly testable
/// against `tests/fixtures/google_chat/members_list.json`'s seven-membership
/// shape without a live API.
///
/// **Capture, not the invite filter.** `EXTERNAL` and `MANAGED_EXTERNAL`
/// stay in [`SpaceRoster::members`]. Message capture must keep seeing those
/// people. Who is invited or seated is [`classify_chat_invite_seat`],
/// applied later by the seat pass — never here.
fn roster_from_memberships(memberships: &[ChatMembership]) -> SpaceRoster {
    let mut roster = SpaceRoster::default();
    let mut seen: HashSet<String> = HashSet::new();
    for m in memberships {
        if m.state != "JOINED" {
            roster.not_joined_excluded += 1;
            continue;
        }
        if m.group_member.is_some() {
            roster.group_memberships += 1;
            continue;
        }
        let Some(member) = &m.member else {
            // Neither a `member` nor a `groupMember` — a shape this API is
            // not documented to produce. Counted as not-joined rather than
            // silently vanishing, so the disclosure's arithmetic still
            // accounts for every row we were handed.
            roster.not_joined_excluded += 1;
            continue;
        };
        if member.user_type.eq_ignore_ascii_case("BOT") {
            roster.bots_excluded += 1;
            continue;
        }
        let sub = bare_sub(member.name.trim()).to_string();
        if sub.is_empty() || !seen.insert(sub.clone()) {
            continue;
        }
        roster.members.push(RosterMember {
            sub,
            display_name: member.display_name.trim().to_string(),
            resource_name: member.name.trim().to_string(),
            user_type: member.user_type.trim().to_string(),
            affiliation: m.affiliation.trim().to_string(),
        });
    }
    roster
}

/// `Membership.affiliation` values that are guests. They are not invited
/// and not seated. Case-insensitive — Google documents the uppercase
/// spellings; a lowercase payload must not fall through to "full member".
fn is_external_affiliation(affiliation: &str) -> bool {
    let affiliation = affiliation.trim();
    affiliation.eq_ignore_ascii_case("EXTERNAL") || affiliation.eq_ignore_ascii_case("MANAGED_EXTERNAL")
}

/// A `HUMAN` row whose `affiliation` field was absent. The only roster
/// shape that may use directory + domain (D2). A bot row also omits the
/// field; that is not this case.
fn human_lacks_affiliation(user_type: &str, affiliation: &str) -> bool {
    user_type.eq_ignore_ascii_case("HUMAN") && affiliation.trim().is_empty()
}

/// The identity written for an external Chat member (item 43).
///
/// `display_name` and `directory_email` are accepted so a caller that has
/// them in hand still cannot persist them: both are discarded. Address and
/// name are absent even when the membership payload or the directory
/// returned them. The key is the full `users/{id}` resource name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalChatMemberRecord {
    key: String,
    name: Option<String>,
    address: Option<String>,
}

fn external_chat_member_record(
    resource_name: &str,
    _display_name: &str,
    _directory_email: Option<&str>,
) -> Option<ExternalChatMemberRecord> {
    let trimmed = resource_name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let key = if let Some(bare) = trimmed.strip_prefix("users/") {
        if bare.is_empty() {
            return None;
        }
        trimmed.to_string()
    } else {
        format!("users/{trimmed}")
    };
    Some(ExternalChatMemberRecord { key, name: None, address: None })
}

/// Invite/seat classification for one already-captured roster member.
///
/// `directory_email` is consulted for the admission decision only when
/// `user_type` is `HUMAN` and `affiliation` is empty. `Err` means the
/// directory could not be read; `Ok(None)` means it was read and this
/// person is not in it. `INTERNAL` never consults it. `EXTERNAL` /
/// `MANAGED_EXTERNAL` never consult it for admission, even when the
/// address's domain matches an anchor — the email is discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChatInviteSeat {
    /// Existing seat path: sub match, then directory email.
    Eligible,
    /// Not invited, not seated. Persist [`ExternalChatMemberRecord`] when
    /// `Some`; the record carries no name and no address.
    External(Option<ExternalChatMemberRecord>),
    /// Checked, and not invited. Not an unmatched-invite candidate.
    NotInvited,
    /// HUMAN with no affiliation, and the directory itself could not be
    /// read. Not invited, and not an unmatched-invite candidate.
    Unavailable,
}

pub(crate) fn classify_chat_invite_seat(
    user_type: &str,
    affiliation: &str,
    resource_name: &str,
    display_name: &str,
    directory_email: Result<Option<&str>, ()>,
    anchor_domains: &[String],
) -> ChatInviteSeat {
    if is_external_affiliation(affiliation) {
        let email = directory_email.ok().flatten();
        return ChatInviteSeat::External(external_chat_member_record(resource_name, display_name, email));
    }
    if affiliation.trim().eq_ignore_ascii_case("INTERNAL") {
        return ChatInviteSeat::Eligible;
    }
    if !affiliation.trim().is_empty() {
        return ChatInviteSeat::NotInvited;
    }
    if !human_lacks_affiliation(user_type, affiliation) {
        return ChatInviteSeat::Eligible;
    }
    let email = match directory_email {
        Err(()) => return ChatInviteSeat::Unavailable,
        Ok(email) => email,
    };
    let Some(email) = email.filter(|value| !value.trim().is_empty()) else {
        return ChatInviteSeat::NotInvited;
    };
    if crate::services::org_domains::address_on_guest_anchor(email, anchor_domains) {
        ChatInviteSeat::Eligible
    } else {
        ChatInviteSeat::NotInvited
    }
}

/// `unmapped` after the invite/seat classifier has taken its people out.
/// A guest who was not invited must not be reported as "genuinely no
/// CoralSwarm account" — that count is what an invite pass emails.
fn unmapped_excluding_not_invited(
    total: usize,
    matched: usize,
    unavailable: usize,
    not_invited: usize,
) -> usize {
    total.saturating_sub(matched).saturating_sub(unavailable).saturating_sub(not_invited)
}

/// Walk EVERY page of `spaces.members.list`. Hitting [`MEMBERS_MAX_PAGES`]
/// is an `Err`, never a short roster — *"an empty channel and an
/// inaccessible one must never look identical"*
/// (`connectors::slack::fetch_conversation_members`'s rule, §1.10).
async fn list_members_all(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
) -> Result<Vec<ChatMembership>, ConnectorError> {
    let mut out = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..MEMBERS_MAX_PAGES {
        let page = list_members_page(http, access_token, space_id, page_token.as_deref()).await?;
        out.extend(page.memberships);
        match page.next_page_token.filter(|t| !t.is_empty()) {
            Some(next) => page_token = Some(next),
            None => return Ok(out),
        }
    }
    Err(ConnectorError::Exchange(format!(
        "chat spaces.members.list for {space_id} exceeded {MEMBERS_MAX_PAGES} pages — refusing to \
         report a partial roster as a complete one"
    )))
}

/// The full roster read + count. `Err` on ANY unestablishable roster —
/// *"a disclosure with a missing count is better than a wrong one"*, and
/// *"an unknown roster is never treated as an empty one"* (§3.4).
pub(crate) async fn resolve_space_roster(
    http: &reqwest::Client,
    access_token: &str,
    space_id: &str,
) -> Result<SpaceRoster, ConnectorError> {
    Ok(roster_from_memberships(&list_members_all(http, access_token, space_id).await?))
}

// ---------------------------------------------------------------------------
// Domain directory (People API) — C5 verified-email identity matching
// (chat-per-user-setup PR, 2026-09-20)
// ---------------------------------------------------------------------------
//
// The Chat API itself never returns an email address on any resource under
// any scope (§2.4, confirmed against the committed fixture
// `tests/fixtures/google_chat/members_list.json` — that fact has not
// changed). What changed: `spaces.members.list`'s `User.name`
// (`"users/{id}"`) and the People API's `Person.resourceName`
// (`"people/{id}"`) carry the SAME bare numeric Google account id — the
// exact equivalence `resolve_google_identity_in_account`'s match against
// `oauth_tokens.provider_user_id` already depends on. So a roster member
// Chat itself cannot name an email for can still be looked up in the
// Workspace's own domain directory via `people.listDirectoryPeople` — a
// DIFFERENT Google API, reached with the `directory.readonly` scope this
// PR adds to `Surface::Chat` (see `connectors::google::Surface::Chat::
// data_scope`'s own doc comment for the re-consent cost that widening
// deliberately accepts).
//
// **Two preconditions this connector does not control, and cannot tell
// apart from each other OR from an ordinary transient failure by
// inspecting one HTTP response alone:**
//   1. The connecting member's OAuth grant must actually include
//      `directory.readonly` — anyone who connected `google_chat` BEFORE
//      this PR shipped has the old three-scope grant and needs to
//      reconnect before this call can ever succeed for them.
//   2. Google's own documented precondition for `listDirectoryPeople`: the
//      Workspace's domain admin must have enabled "external contact and
//      profile sharing" for the domain. If they have not, the call
//      SUCCEEDS (HTTP 200) and returns ZERO people — indistinguishable, at
//      the wire level, from a domain that has (implausibly) opted zero
//      accounts into directory sharing.
//
// [`fetch_domain_directory`] folds BOTH of the above, plus every ordinary
// transport failure, into a single `Err` — see its own doc comment for why
// an empty page-1 result is treated as a failure rather than a
// confirmed-empty directory. This is the same distinction PR 1 (Slack OAuth
// scope reconciliation, `connectors::slack::SlackIdentity`) fixed one layer
// up for Slack: a bot token lacking `users:read.email` made Slack return
// `ok: true` with the email field silently omitted, which an earlier
// version of that matcher read as "this member has no CoralSwarm account"
// instead of "this token cannot answer that question for anyone" — and
// since the invite-the-unmatched feature emails exactly the members
// reported `Unmatched`, that bug would have emailed entire workspaces.
// Nothing in this PR sends email, but [`ChatVerifiedIdentity`] is
// deliberately the same three-outcome shape for exactly that reason: a
// future caller must not be able to make the identical mistake here.

const PEOPLE_API_BASE: &str = "https://people.googleapis.com/v1";

/// Google's documented max `pageSize` for `people.listDirectoryPeople`.
/// Mirrors [`LIST_PAGE_SIZE`]'s role for `spaces.members.list`.
const DIRECTORY_PAGE_SIZE: u32 = 1000;

/// Mirrors [`MEMBERS_MAX_PAGES`]'s role and its exact reasoning — *"an empty
/// channel and an inaccessible one must never look identical"* — hitting
/// this cap is an `Err`, never a directory silently truncated to its first
/// 25,000 entries.
const DIRECTORY_MAX_PAGES: usize = 25;

/// One domain-directory entry `people.listDirectoryPeople` returned for a
/// single Google account — the two fields [`match_and_persist_chat_verified_
/// email`] needs: the address to match against `users.email`, and a display
/// name to seed a fresh `connector_identities` row with (mirrors
/// `extract_verified_slack_profile`'s `canonical_name_hint`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryEntry {
    pub(crate) email: String,
    pub(crate) display_name: String,
}

/// A [`fetch_domain_directory`] outcome, cached for the lifetime of one
/// sweep pass and keyed on the elected reader whose `access_token` the fetch
/// used — CodeRabbit round 1 (#1005, Thread 23, Performance & Scalability).
///
/// `fetch_domain_directory` is a paginated People API walk (up to
/// `DIRECTORY_MAX_PAGES` requests); before this cache, `auto_seat_space_members`
/// re-ran it once per SPACE that needed it, so a reader whose access token
/// backs several routed spaces in the same account paid for the same walk
/// once per space instead of once per pass. Callers create one of these at
/// the sweep-pass boundary (`sweep_account`, one per account per tick) and
/// thread it through every `sync_space_once` call in that pass; a one-space
/// caller (the push-event handlers in `routes::google_push`) constructs a
/// fresh, single-entry one per call, which is correct too — there is only
/// ever one reader to cache in that scope.
///
/// The cached value is a `Result`, not only the `Ok` case: an `Err` (rate
/// limited, missing scope, sharing disabled) is exactly as expensive to
/// produce as a success — it still walked every page `fetch_domain_directory`
/// was willing to try before giving up — so it is cached too, per this
/// finding's own text: *"Cache the result, including an unavailable result,
/// so each reader is fetched at most once per pass."*
pub(crate) type DomainDirectoryCache = HashMap<Uuid, Result<HashMap<String, DirectoryEntry>, String>>;

/// `EmailAddress.metadata.source.type` — CodeRabbit round 1 (#1005, Security
/// & Privacy): only a value asserted from `DOMAIN_PROFILE` — the Workspace
/// admin's own directory record, the same source `sources=
/// DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE` restricts the REQUEST to — is
/// trusted here. Deliberately compared as a raw string against Google's
/// documented enum value rather than parsed into a Rust enum: an unrecognised
/// future value must fail closed (never match), not silently deserialize to
/// some default variant.
const DOMAIN_PROFILE_SOURCE: &str = "DOMAIN_PROFILE";

#[derive(Debug, Deserialize)]
struct DirectorySource {
    #[serde(default, rename = "type")]
    source_type: String,
}

/// `EmailAddress.metadata` — CodeRabbit round 1 (#1005, Security & Privacy).
/// `verified`: "True if the field is verified... typically a name, email
/// address, phone number, or website that has been confirmed to be owned by
/// the person" (Google's own field doc). Before this fix,
/// `DirectoryEmailAddress` kept only `value` — the FIRST nonblank address
/// reached [`match_and_persist_chat_verified_email`] with no verification
/// check at all, so an unverified, user-editable directory field could
/// match `users.email` and be persisted with `link_method = 'verified_email'`
/// — the exact guarantee that link method name promises and this one
/// silently did not keep.
#[derive(Debug, Default, Deserialize)]
struct DirectoryFieldMetadata {
    #[serde(default)]
    verified: bool,
    #[serde(default)]
    source: Option<DirectorySource>,
}

#[derive(Debug, Deserialize)]
struct DirectoryEmailAddress {
    #[serde(default)]
    value: String,
    /// Absent metadata fails CLOSED (`is_verified_domain_profile` below
    /// requires `Some` and `verified == true`), never falls back to trusting
    /// an unqualified `value` — same "no metadata, no trust" posture the
    /// module doc's other `Unavailable`-vs-silent-non-match distinctions take
    /// throughout this file.
    #[serde(default)]
    metadata: Option<DirectoryFieldMetadata>,
}

impl DirectoryEmailAddress {
    /// `true` only for a field BOTH verified AND sourced from the domain
    /// profile — see [`DirectoryFieldMetadata`]'s own doc comment for why
    /// both checks exist rather than either alone.
    fn is_verified_domain_profile(&self) -> bool {
        self.metadata.as_ref().is_some_and(|m| {
            m.verified && m.source.as_ref().is_some_and(|s| s.source_type == DOMAIN_PROFILE_SOURCE)
        })
    }
}

#[derive(Debug, Deserialize)]
struct DirectoryName {
    #[serde(default, rename = "displayName")]
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct DirectoryPerson {
    #[serde(default, rename = "resourceName")]
    resource_name: String,
    #[serde(default, rename = "emailAddresses")]
    email_addresses: Vec<DirectoryEmailAddress>,
    #[serde(default)]
    names: Vec<DirectoryName>,
}

#[derive(Debug, Deserialize)]
struct ListDirectoryPeopleResponse {
    #[serde(default)]
    people: Vec<DirectoryPerson>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// One page of `people:listDirectoryPeople`. `readMask=emailAddresses,names`
/// is the minimum that buys both fields [`DirectoryEntry`] needs — each
/// returned `emailAddresses[]` entry's OWN nested `metadata` (`verified`/
/// `source`, [`DirectoryFieldMetadata`]) is intrinsic to the `EmailAddress`
/// object and comes back whenever the field group itself is requested; it
/// is not a separate top-level `metadata` mask value (that name instead
/// selects `Person`-level metadata — `sources`/`objectType` — a different,
/// unrelated field this connector does not need). `sources=
/// DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE` restricts the read to the caller's
/// own Workspace domain roster — never a personal-contacts merge, which a
/// second `sources` value would pull in and which this connector has no
/// business reading.
async fn list_directory_people_page(
    http: &reqwest::Client,
    access_token: &str,
    page_token: Option<&str>,
) -> Result<ListDirectoryPeopleResponse, ConnectorError> {
    let mut url = format!(
        "{PEOPLE_API_BASE}/people:listDirectoryPeople?readMask=emailAddresses,names&\
         sources=DIRECTORY_SOURCE_TYPE_DOMAIN_PROFILE&pageSize={DIRECTORY_PAGE_SIZE}"
    );
    if let Some(pt) = page_token {
        url.push_str("&pageToken=");
        url.push_str(&urlencoding::encode(pt));
    }
    let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!("people.listDirectoryPeople failed: {status}")));
    }
    resp.json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("people.listDirectoryPeople parse: {e}")))
}

/// Walk EVERY page. Hitting [`DIRECTORY_MAX_PAGES`] is an `Err`, never a
/// short directory — the same rule [`list_members_all`] applies to a
/// space's roster, for the identical reason ("every page or an `Err`").
async fn list_directory_people_all(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<Vec<DirectoryPerson>, ConnectorError> {
    let mut out = Vec::new();
    let mut page_token: Option<String> = None;
    for _ in 0..DIRECTORY_MAX_PAGES {
        let page = list_directory_people_page(http, access_token, page_token.as_deref()).await?;
        out.extend(page.people);
        match page.next_page_token.filter(|t| !t.is_empty()) {
            Some(next) => page_token = Some(next),
            None => return Ok(out),
        }
    }
    Err(ConnectorError::Exchange(format!(
        "people.listDirectoryPeople exceeded {DIRECTORY_MAX_PAGES} pages — refusing to report a \
         partial directory as a complete one"
    )))
}

/// Pure `Vec<DirectoryPerson>` → `sub -> DirectoryEntry` map, factored out so
/// the id-stripping/blank-filtering/name-fallback rules are unit-testable
/// without a live People API call (this crate has no HTTP-mock harness —
/// same constraint `connectors::slack`'s own tests document) — directly
/// mirroring how [`roster_from_memberships`] is factored out from
/// [`list_members_all`] for the identical reason.
///
/// A person with no usable email (every `emailAddresses` entry blank, or the
/// list empty — Google's own note: a directory profile may have visibility
/// restrictions per-field) is dropped rather than inserted with an empty
/// string, so a later exact-match lookup can never accidentally key on `""`.
///
/// **Only a VERIFIED, domain-profile-sourced address is usable**
/// (CodeRabbit round 1, #1005, Security & Privacy) —
/// [`DirectoryEmailAddress::is_verified_domain_profile`]. An unverified or
/// non-domain-profile address is treated exactly like a blank one: dropped,
/// never a reason to fall through to a lower-priority entry that IS
/// verified — Google returns `emailAddresses` in no documented priority
/// order, so silently preferring "first present" over "first verified"
/// would be its own bug.
fn directory_map_from_people(people: Vec<DirectoryPerson>) -> HashMap<String, DirectoryEntry> {
    let mut map = HashMap::with_capacity(people.len());
    for person in people {
        let sub = person.resource_name.strip_prefix("people/").unwrap_or(&person.resource_name).trim();
        if sub.is_empty() {
            continue;
        }
        let Some(email) = person
            .email_addresses
            .iter()
            .filter(|e| e.is_verified_domain_profile())
            .map(|e| e.value.trim())
            .find(|v| !v.is_empty())
        else {
            continue;
        };
        let display_name = person
            .names
            .iter()
            .map(|n| n.display_name.trim())
            .find(|v| !v.is_empty())
            .unwrap_or(email);
        map.insert(
            sub.to_string(),
            DirectoryEntry {
                email: email.to_string(),
                display_name: display_name.to_string(),
            },
        );
    }
    map
}

/// Fetch the caller's Workspace domain directory as a `sub -> DirectoryEntry`
/// map, keyed exactly like Chat's own roster (`RosterMember::sub`).
///
/// **An empty result is folded into `Err`, not returned as `Ok(<empty
/// map>)`.** This is this section's central point, restated here because it
/// is the one property a caller MUST NOT get wrong: Google returns HTTP 200
/// with zero `people` both when this domain's admin has not enabled external
/// contact and profile sharing (the documented precondition for this API)
/// AND when a pre-widening connection's token still lacks
/// `directory.readonly` entirely (Google omits inaccessible fields rather
/// than erroring — the same shape Slack's `users.info` uses for a scope it
/// lacks) — there is no wire-level way to tell either of those apart from a
/// domain that has genuinely opted zero accounts into directory sharing.
/// Treating a zero-length page 1 as a confirmed-empty directory would read
/// as "nobody in this space has a CoralSwarm account", which is false in
/// both of the likely real cases. A real, sharing-enabled Workspace domain
/// directory contains at minimum the calling account's own profile, so zero
/// results is itself the failure signal — a deliberate, disclosed
/// heuristic, not a documented Google guarantee.
pub(crate) async fn fetch_domain_directory(
    http: &reqwest::Client,
    access_token: &str,
) -> Result<HashMap<String, DirectoryEntry>, ConnectorError> {
    let people = list_directory_people_all(http, access_token).await?;
    if people.is_empty() {
        return Err(ConnectorError::Exchange(
            "people.listDirectoryPeople returned zero directory entries — most likely this \
             domain's Workspace admin has not enabled external contact and profile sharing \
             (Google's own precondition for this API), or this connection predates the \
             directory.readonly scope and has never re-consented; treated as UNAVAILABLE, never \
             as \"nobody in this space has a CoralSwarm account\""
                .to_string(),
        ));
    }
    Ok(directory_map_from_people(people))
}

/// The three genuinely different outcomes of resolving one roster member
/// against the domain directory — the Chat analog of `connectors::slack::
/// SlackIdentity`, same three-way discipline and the same reason: a caller
/// that flattens `Unavailable` into `Unmatched` cannot tell "this person has
/// no CoralSwarm account" apart from "we could not ask", and the two must
/// never be confused (this section's module doc explains the concrete
/// failure mode this mirrors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChatVerifiedIdentity {
    Matched(Uuid),
    Unmatched,
    Unavailable(String),
}

/// Resolve one roster member's verified-email identity against an
/// ALREADY-fetched domain directory — [`fetch_domain_directory`] is called
/// AT MOST ONCE per roster-reconcile pass (by [`auto_seat_space_members`]),
/// never once per member, so this function takes the fetch's own outcome
/// rather than re-fetching.
///
/// `directory` is `Err(reason)` when the domain directory itself could not
/// be established this pass — every member resolved through this function
/// is `Unavailable(reason)` in that case, NEVER `Unmatched`. A member whose
/// `sub` is simply absent from a SUCCESSFULLY fetched directory (an external
/// guest, a personal Gmail account in an otherwise-Workspace space, or
/// anyone this domain's directory genuinely doesn't list) is `Unmatched` —
/// a real answer about that one person, not a failure.
async fn resolve_chat_directory_identity(
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    sub: &str,
    canonical_name_hint: &str,
    directory: Result<&HashMap<String, DirectoryEntry>, &str>,
) -> ChatVerifiedIdentity {
    let directory = match directory {
        Ok(d) => d,
        Err(reason) => return ChatVerifiedIdentity::Unavailable(reason.to_string()),
    };
    let Some(entry) = directory.get(sub) else {
        return ChatVerifiedIdentity::Unmatched;
    };
    let name_hint = if canonical_name_hint.is_empty() {
        entry.display_name.as_str()
    } else {
        canonical_name_hint
    };
    match match_and_persist_chat_verified_email(
        control_pool,
        content_pool,
        account_id,
        workspace_id,
        sub,
        &entry.email,
        name_hint,
    )
    .await
    {
        Ok(Some(user_id)) => ChatVerifiedIdentity::Matched(user_id),
        Ok(None) => ChatVerifiedIdentity::Unmatched,
        Err(e) => ChatVerifiedIdentity::Unavailable(e),
    }
}

/// Always-write half: `canonical_name` only, for EVERY roster member
/// regardless of match outcome — the writer `workers::google_chat_sweep::
/// mention_directory`'s own doc comment says has never existed ("this read
/// returns zero rows in production ... every Chat mention currently renders
/// `@unknown`"). Mirrors `routes::slack_events`'s plain canonical-name
/// upsert (the Slack analog behind `connector_identities`' "populated from
/// connector data at ingest" role, migration `0049`'s own header):
/// deliberately does NOT touch `user_id`/`link_method`/`link_verified_at` —
/// neither in `VALUES` (they default to `NULL` on a fresh row, satisfying
/// migration `0104`'s all-three-or-none CHECK) nor in `DO UPDATE SET` — so a
/// link CONFIRMED by [`match_and_persist_chat_verified_email`] on an earlier
/// or later pass is never clobbered by this function running in between.
async fn persist_chat_canonical_name(
    content_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
    canonical_name: &str,
) -> Result<(), String> {
    if canonical_name.is_empty() || workspace_id.is_empty() || external_user_id.is_empty() {
        // §2.4's UNVERIFIED note: `displayName` may be absent. An empty
        // canonical_name would just make a future `@` mention render blank
        // instead of `@unknown` — no improvement, so skip rather than write
        // a useless row.
        return Ok(());
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(content_pool, account_id)
        .await
        .map_err(|e| format!("persist_chat_canonical_name scoped tx: {e}"))?;
    sqlx::query(
        "INSERT INTO connector_identities (provider, workspace_id, external_user_id, canonical_name) \
         VALUES ('google_chat', $1, $2, $3) \
         ON CONFLICT (provider, workspace_id, external_user_id) \
         DO UPDATE SET canonical_name = EXCLUDED.canonical_name, updated_at = now()",
    )
    .bind(workspace_id)
    .bind(external_user_id)
    .bind(canonical_name)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("persist_chat_canonical_name: {e}"))?;
    tx.commit().await.map_err(|e| format!("persist_chat_canonical_name commit: {e}"))
}

/// Item 43. Persist `users/{id}` for an external member and nothing else.
/// `canonical_name` is empty — the name is absent — and `user_id` is left
/// null so no address is attached. `ON CONFLICT DO NOTHING` so a later pass
/// cannot back-fill a display name or a verified-email link onto this key.
async fn persist_external_chat_member_key(
    content_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
) -> Result<(), String> {
    if workspace_id.is_empty() || external_user_id.is_empty() || !external_user_id.starts_with("users/") {
        return Ok(());
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(content_pool, account_id)
        .await
        .map_err(|e| format!("persist_external_chat_member_key scoped tx: {e}"))?;
    sqlx::query(
        "INSERT INTO connector_identities (provider, workspace_id, external_user_id, canonical_name) \
         VALUES ('google_chat', $1, $2, '') \
         ON CONFLICT (provider, workspace_id, external_user_id) DO NOTHING",
    )
    .bind(workspace_id)
    .bind(external_user_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("persist_external_chat_member_key: {e}"))?;
    tx.commit().await.map_err(|e| format!("persist_external_chat_member_key commit: {e}"))
}

/// D2's anchors for a HUMAN row that has no `affiliation`: every verified
/// org domain, or the domain of the connected member whose token is reading
/// this roster while that list is empty. A failed verified-domain lookup
/// returns nothing and does not fall through to the bootstrap domain —
/// could not ask is not "nothing is verified."
pub(crate) async fn chat_invite_anchor_domain(
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Vec<String> {
    let verified = crate::services::org_domains::verified_domains(control_pool, account_id)
        .await
        .map_err(|e| e.to_string());
    if let Err(e) = &verified {
        tracing::warn!(
            error = %e, %account_id,
            "google_chat: verified org-domain lookup failed — no anchor for a membership \
             whose affiliation is absent (fail closed, not the connecting member's domain)"
        );
    }
    let bootstrap = if matches!(&verified, Ok(domains) if domains.is_empty()) {
        let email: Option<String> = sqlx::query_scalar(
            "SELECT email FROM users WHERE id = $1 AND deleted_at IS NULL AND email IS NOT NULL",
        )
        .bind(owner_user_id)
        .fetch_optional(control_pool)
        .await
        .ok()
        .flatten();
        email.as_deref().and_then(crate::services::org_domains::email_domain_part)
    } else {
        None
    };
    crate::services::org_domains::guest_anchor_domains(verified, bootstrap)
}

/// The verified-link write half — the Chat analog of `connectors::slack::
/// match_and_persist_verified_email`; see that function's own doc comment
/// for the fuller reasoning this mirrors (the asymmetric upsert, why an
/// unmatched lookup writes nothing, the best-effort non-fatal persist).
/// `directory_email` is whatever [`fetch_domain_directory`] returned for
/// this member's `sub` — already known non-empty
/// ([`directory_map_from_people`] drops any entry without one).
///
/// Matches against `users.email` using the SAME normalisation
/// ([`crate::connectors::slack::normalize_email`] — lowercase + trim ONLY,
/// reused rather than re-implemented so a Gmail dot/plus-alias cannot
/// quietly start matching on one connector and not the other).
///
/// Deliberately writes `link_method = 'verified_email'` — the ONLY value
/// migration `0104`'s CHECK permits — and NOT a new `'google_sub'` method.
/// This function is reached ONLY through the domain-directory path, never
/// for a member already resolved via `resolve_google_identity_in_account`'s
/// `oauth_tokens` join (which needs no `connector_identities` row at all to
/// answer "who is this" for its own account-scoped purpose) — and the
/// address it matched on genuinely came from the domain's own directory
/// record for that account, a verified address in the same sense Slack's
/// matcher already uses that label for, not a self-asserted one.
///
/// **Content-plane write via a scoped transaction** (not a bare
/// `.execute(content_pool)`, unlike this function's Slack sibling): pins
/// `search_path` before the write (`tenancy::scope::begin_account_scoped`,
/// same fix as migration `0092`/`#997`), defending against exactly the
/// Neon-pooler-hands-back-an-empty-search_path failure mode that PR closed,
/// for a code path new enough to just build it in rather than inherit
/// Slack's pre-#997 shape.
async fn match_and_persist_chat_verified_email(
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    external_user_id: &str,
    directory_email: &str,
    canonical_name_hint: &str,
) -> Result<Option<Uuid>, String> {
    let normalized = crate::connectors::slack::normalize_email(directory_email);
    if normalized.is_empty() {
        return Ok(None);
    }

    // `Ok(None)` means "asked, and no CoralSwarm user has this address" —
    // the ordinary guest/contractor outcome. `Err` means the question could
    // not be asked at all — collapsing the two would report a database
    // outage as a confident "this person has no account" (identical
    // reasoning to `connectors::slack::match_and_persist_verified_email`'s
    // own doc comment).
    let user_id: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM users WHERE lower(trim(email)) = $1 AND deleted_at IS NULL")
            .bind(&normalized)
            .fetch_optional(control_pool)
            .await
            .map_err(|e| format!("users email lookup: {e}"))?
            .flatten();
    let Some(user_id) = user_id else {
        return Ok(None);
    };

    let mut tx = crate::tenancy::scope::begin_account_scoped(content_pool, account_id)
        .await
        .map_err(|e| format!("match_and_persist_chat_verified_email scoped tx: {e}"))?;
    let write = sqlx::query(
        "INSERT INTO connector_identities \
             (provider, workspace_id, external_user_id, canonical_name, user_id, \
              link_method, link_verified_at) \
         VALUES ('google_chat', $1, $2, $3, $4, 'verified_email', now()) \
         ON CONFLICT (provider, workspace_id, external_user_id) DO UPDATE SET \
             user_id          = EXCLUDED.user_id, \
             link_method      = EXCLUDED.link_method, \
             link_verified_at = EXCLUDED.link_verified_at, \
             updated_at       = now()",
    )
    .bind(workspace_id)
    .bind(external_user_id)
    .bind(canonical_name_hint)
    .bind(user_id)
    .execute(&mut *tx)
    .await;
    match write {
        Ok(_) => {
            tx.commit().await.map_err(|e| format!("match_and_persist_chat_verified_email commit: {e}"))?;
        }
        Err(e) => {
            // Best-effort, same as Slack's matcher: the caller already has
            // its answer (`Some(user_id)`), and losing the persistence
            // optimization once is far cheaper than failing the whole
            // resolution over a non-essential write.
            tx.rollback().await.ok();
            tracing::warn!(
                error = %e, workspace_id, external_user_id,
                "match_and_persist_chat_verified_email: failed to persist confirmed link (non-fatal)"
            );
        }
    }
    Ok(Some(user_id))
}

/// The connecting member's own Google account email —
/// `oauth_tokens.provider_workspace_id` for their `google_chat` token — the
/// SAME value `workers::google_chat_sweep::account_chat_workspace_ids`
/// collects across every member an account has connected. Used as the
/// [`connector_identities`] `workspace_id` anchor for rows this pass writes,
/// so a write made under THIS member's connection always falls inside the
/// set `account_chat_workspace_ids` returns for this same account — the
/// precondition `workers::google_chat_sweep::mention_directory`'s
/// `workspace_id = ANY($1)` read depends on. `None` for a legacy/malformed
/// row with no `provider_workspace_id` on file — identity writes are
/// skipped entirely for a pass with no anchor rather than guessing one.
async fn reader_workspace_id(
    control_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Option<String>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("reader_workspace_id scoped tx: {e}")))?;
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT provider_workspace_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'google_chat' AND owner_user_id = $2 \
           AND provider_workspace_id <> '' \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("reader_workspace_id: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(w,)| w))
}

/// The Chat workspace ids this account has actually connected — EVERY
/// connecting member's own [`reader_workspace_id`], collected. The anchor
/// `workers::google_chat_sweep::mention_directory` and
/// [`resolve_chat_fanout_destinations`] both scope their `connector_
/// identities` reads on, and the same set [`reader_workspace_id`]'s own doc
/// comment promises a roster-pass write always falls inside.
///
/// **Moved here from `workers::google_chat_sweep` (coordinator review,
/// chat-per-user-setup PR, 2026-09-20)** so the connector owns its own
/// `connector_identities` anchor logic rather than a sibling worker module
/// owning it and the connector needing to reach upward into `workers` to
/// reuse it — that direction of dependency does not exist anywhere else in
/// this tree. `workers::google_chat_sweep::mention_directory` now calls
/// this function instead of a private copy of the identical query; a second
/// implementation of "which workspace ids does this account own" is exactly
/// how the roster-writer's anchor and the mention-reader's anchor could
/// silently drift apart.
pub(crate) async fn account_chat_workspace_ids(
    control_pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<String>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("account_chat_workspace_ids scoped tx: {e}")))?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT provider_workspace_id FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'google_chat' AND provider_workspace_id <> ''",
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("account_chat_workspace_ids: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(w,)| w).collect())
}

/// What one seating pass did — every number the §5.4 disclosure needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SeatingOutcome {
    pub(crate) total_humans: usize,
    /// `N` — matched CoralSwarm identities that ALSO pass
    /// `EXISTS(account_members …)`.
    pub(crate) matched: usize,
    pub(crate) seated: usize,
    /// **Derived, never an independent `count(*)`** —
    /// `total.saturating_sub(matched.len()).saturating_sub(unavailable)`,
    /// where `matched` is a set of MEMBERS, not of graphs. `slack.rs:561`-
    /// `:570` is the bug this rule exists to prevent: computing `unmapped`
    /// from a separate query while the matched set came from a live
    /// resolution produced *"a `connector_health` warning claiming N members
    /// are unmapped when they were just successfully seated."*
    ///
    /// **Means "we asked, and this person genuinely has no CoralSwarm
    /// account" — nothing else** (coordinator review, chat-per-user-setup
    /// PR, 2026-09-20). Before [`unavailable`](Self::unavailable) existed,
    /// this field silently absorbed BOTH that case AND "the domain directory
    /// itself could not be read this pass" — the exact mislabel PR 1 (Slack
    /// OAuth scope reconciliation) fixed one connector over: a bot token
    /// lacking `users:read.email` made every member read as `Unmatched`,
    /// and because the invite-the-unmatched feature emails exactly the
    /// members reported unmatched, that bug would have emailed entire
    /// workspaces. Chat has no such feature TODAY — which is precisely why
    /// the distinction has to be carried in the RETURNED COUNTS now, before
    /// a future feature reads `unmapped` and makes the identical mistake
    /// with nothing here to stop it.
    pub(crate) unmapped: usize,
    /// Roster members whose identity could not even be CHECKED this pass:
    /// the sub match found nothing AND the domain directory itself was
    /// `Unavailable` ([`ChatVerifiedIdentity::Unavailable`]) — never a
    /// member the directory was successfully read for and simply did not
    /// list (that is a real `unmapped`, not this). A wholesale directory
    /// failure (scope not granted on a pre-widening connection, or the
    /// domain admin has not enabled external contact/profile sharing) shows
    /// up here as `unavailable == total_humans - matched`, NOT as a spike in
    /// `unmapped` — see that field's own doc comment for why conflating the
    /// two is the specific mistake this field exists to make impossible.
    pub(crate) unavailable: usize,
    pub(crate) group_memberships: usize,
    /// The matched CoralSwarm user ids themselves — what the sweep's
    /// diff-and-revoke pass subtracts outstanding grants against (security
    /// review, M2). Returned from the SAME roster read that produced
    /// `matched`, so the grant and revoke halves can never disagree about
    /// who is still in the space, and a failed roster read yields neither.
    pub(crate) matched_user_ids: Vec<Uuid>,
}

/// [`matched_members_for_roster`]'s return shape — the matched ids AND how
/// many roster members could not even be CHECKED this pass.
///
/// **Why a struct and not a bare `Vec<Uuid>` (coordinator review,
/// chat-per-user-setup PR, 2026-09-20).** `SeatingOutcome::unmapped`'s own
/// doc comment names the mistake this exists to prevent: deriving `unmapped`
/// as `total - matched.len()` alone silently folds "genuinely has no
/// CoralSwarm account" (`ChatVerifiedIdentity::Unmatched`, or no
/// `workspace_id` anchor) together with "the domain directory itself could
/// not be read this pass" (`ChatVerifiedIdentity::Unavailable`) — the exact
/// `Unmatched`/`Unavailable` conflation PR 1 fixed one connector over for
/// Slack. Carrying `unavailable` back out of this function, separately from
/// `matched`, is what lets [`auto_seat_space_members`] compute BOTH
/// `SeatingOutcome::unmapped` and `SeatingOutcome::unavailable` correctly
/// instead of only being able to reconstruct their SUM.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RosterMatchOutcome {
    /// Matched CoralSwarm user ids that ALSO pass the `account_members`
    /// gate.
    matched: Vec<Uuid>,
    /// Roster members whose identity could not even be CHECKED this pass:
    /// the sub match found nothing, AND either there was no `workspace_id`
    /// anchor to check the directory with at all, or the directory itself
    /// was [`ChatVerifiedIdentity::Unavailable`]. Never incremented for a
    /// member the directory was successfully read for and simply did not
    /// list — that member is `Unmatched`, folded into `unmapped` at the
    /// caller, not here.
    unavailable: usize,
    /// EXTERNAL / MANAGED_EXTERNAL, plus HUMANS whose missing affiliation
    /// failed the directory + domain check. Subtracted from `unmapped` so
    /// an invite pass cannot email them.
    not_invited: usize,
}

/// Resolve a space's roster to `(matched CoralSwarm user ids, total humans,
/// group memberships)`.
///
/// **Read order is an access-control property, not a performance detail**
/// (§1.10 lesson 2). Slack reads identities before members because
/// members-first would land a message in a just-REMOVED member's private
/// ocean — *"an access-control failure, not a stale-read annoyance"*. Chat's
/// two reads both live on the control plane (`oauth_tokens` for identity,
/// `account_members` for the gate), so the physical split Slack has does not
/// arise — but the ORDER is kept identical anyway, so the rule survives
/// G4.6 moving one of them.
///
/// A DB error on the `account_members` gate **propagates**; it is never
/// `unwrap_or(false)`. That collapse is what made a database failure look
/// exactly like "this user is not a member of the account", silently seating
/// fewer people than the disclosure promised, with nothing logged
/// (`slack.rs:1461`-`:1468`).
///
/// **`workspace_id`/`directory` (chat-per-user-setup PR, 2026-09-20).** The
/// sub-based match (step 1) is ALWAYS tried first — it needs no extra call
/// and no admin precondition. Only a member step 1 could not place falls
/// through to the domain-directory match (step 1b): `workspace_id` is the
/// `connector_identities` anchor ([`reader_workspace_id`]'s own doc
/// comment), `None` when no anchor is on file (identity writes — both the
/// always-write canonical name and any verified link — are skipped for that
/// member entirely, and they simply stay unmatched-by-directory); `directory`
/// is [`fetch_domain_directory`]'s own outcome, fetched ONCE by the caller
/// ([`auto_seat_space_members`]) and threaded through so this function stays
/// network-free and DB-only, matching every other identity gate here. A
/// `directory` `Err` never gets counted as "no CoralSwarm account" — see
/// [`resolve_chat_directory_identity`] and this file's "Domain directory"
/// section for why, and why that distinction is load-bearing.
async fn matched_members_for_roster(
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    workspace_id: Option<&str>,
    roster: &SpaceRoster,
    directory: Result<&HashMap<String, DirectoryEntry>, &str>,
    anchor_domains: &[String],
) -> Result<RosterMatchOutcome, String> {
    let mut matched: Vec<Uuid> = Vec::new();
    let mut unavailable: usize = 0;
    let mut not_invited: usize = 0;
    let mut seen: HashSet<Uuid> = HashSet::new();
    for member in &roster.members {
        let directory_email = match directory {
            Err(_) => Err(()),
            Ok(map) => Ok(map.get(&member.sub).map(|entry| entry.email.as_str())),
        };
        // Invite/seat gate (items 42 and 43). Capture already kept this
        // person on the roster; this is the only place a guest is skipped.
        match classify_chat_invite_seat(
            &member.user_type,
            &member.affiliation,
            &member.resource_name,
            &member.display_name,
            directory_email,
            anchor_domains,
        ) {
            ChatInviteSeat::External(record) => {
                if let (Some(workspace_id), Some(record)) = (workspace_id, record) {
                    if let Err(e) =
                        persist_external_chat_member_key(content_pool, account_id, workspace_id, &record.key).await
                    {
                        tracing::warn!(
                            error = %e, %account_id, key = %record.key,
                            "google_chat: failed to persist external member key (non-fatal)"
                        );
                    }
                }
                not_invited += 1;
                continue;
            }
            ChatInviteSeat::NotInvited => {
                not_invited += 1;
                continue;
            }
            ChatInviteSeat::Unavailable => {
                unavailable += 1;
                tracing::info!(
                    %account_id, sub = %member.sub,
                    "google_chat: affiliation is absent and the directory could not be read — \
                     not invited, counted as unavailable, never as unmapped"
                );
                continue;
            }
            ChatInviteSeat::Eligible => {}
        }

        // Canonical name for an eligible member. External members never
        // reach here: their key is `users/{id}` and their name is absent.
        if let Some(workspace_id) = workspace_id {
            if let Err(e) =
                persist_chat_canonical_name(content_pool, account_id, workspace_id, &member.sub, &member.display_name)
                    .await
            {
                tracing::warn!(
                    error = %e, %account_id, sub = %member.sub,
                    "google_chat: failed to persist connector_identities canonical name (non-fatal — \
                     mentions for this member may keep rendering @unknown until a later pass succeeds)"
                );
            }
        }

        // 1. Identity (§3.3) — ACCOUNT-SCOPED. Never the cross-account
        //    `resolve_google_identity_by_sub`: that is the `ADDED_TO_SPACE`
        //    bootstrap ONLY, and using it here was #725's M3 defect (it
        //    counted anyone connected under an unrelated account).
        let user_id = match resolve_google_identity_in_account(control_pool, account_id, &member.sub).await {
            Ok(Some(id)) => Some(id),
            Ok(None) => match workspace_id {
                // 1b. The domain-directory fallback (C5 verified-email
                //     match) — ONLY reached when the sub match above found
                //     nothing, and only when an anchor exists to write a
                //     confirmed link against.
                Some(workspace_id) => {
                    match resolve_chat_directory_identity(
                        control_pool,
                        content_pool,
                        account_id,
                        workspace_id,
                        &member.sub,
                        &member.display_name,
                        directory,
                    )
                    .await
                    {
                        ChatVerifiedIdentity::Matched(id) => Some(id),
                        ChatVerifiedIdentity::Unmatched => None,
                        ChatVerifiedIdentity::Unavailable(reason) => {
                            // Could-not-check, NOT a confirmed non-account —
                            // counted separately so the caller's disclosure
                            // never folds this into `unmapped` (this
                            // function's own doc comment / `RosterMatchOutcome`'s).
                            unavailable += 1;
                            tracing::info!(
                                %account_id, sub = %member.sub, reason = %reason,
                                "google_chat: domain-directory identity lookup unavailable for this \
                                 member this pass — counted as unavailable, never as unmapped or as \
                                 a confirmed non-account"
                            );
                            None
                        }
                    }
                }
                // No anchor to check the directory with at all — equally
                // "could not check", not "checked and there is no account".
                None => {
                    unavailable += 1;
                    None
                }
            },
            Err(e) => return Err(format!("identity resolution for {}: {e}", member.sub)),
        };
        let Some(user_id) = user_id else { continue };
        // 2. The account_members gate.
        let is_member: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)")
                .bind(account_id)
                .bind(user_id)
                .fetch_one(control_pool)
                .await
                .map_err(|e| format!("account_members check for {user_id}: {e}"))?;
        if is_member && seen.insert(user_id) {
            matched.push(user_id);
        }
    }
    Ok(RosterMatchOutcome { matched, unavailable, not_invited })
}

/// Seat every `JOINED`, non-bot, matched roster member of `space_id` on
/// `team_id` (§3.4/D7, I4). Structurally
/// `routes::slack_interactivity::auto_seat_channel_members`, with Slack's
/// counting discipline verbatim and its write half REUSED rather than
/// re-implemented (`seat_members_for_source` — *"a second implementation of
/// 'seat these people and record why' is how the grant and revoke halves
/// drift apart"*).
///
/// Provenance is `'google_chat:{space_id}'` ([`crate::seats::
/// google_chat_space_source`]), namespaced exactly as
/// `'slack:{channel_id}'`/`'teams:{channel_id}'` are, so
/// [`revoke_seat_grant`](crate::seats::revoke_seat_grant) on a
/// `membership.v1.deleted` revokes what THIS space granted and nothing else
/// (conformance test 7).
///
/// `Err` is reserved for *"the roster could not be walked"* — never for a
/// per-member write failure, which `seat_members_for_source` isolates
/// per-member by design.
///
/// **`owner_user_id` (chat-per-user-setup PR, 2026-09-20).** The reader
/// whose `access_token` this call was handed — needed to resolve
/// [`reader_workspace_id`], the `connector_identities` anchor for any
/// identity row this pass writes. The directory fetch itself
/// ([`fetch_domain_directory`]) is SKIPPED outright — no network call made
/// at all — when a cheap, network-free pre-scan finds every roster member
/// already resolves via the sub match; this keeps the common case (an
/// account whose members have each connected their own Google Chat) free of
/// any People API round trip. The pre-scan re-runs `resolve_google_identity_
/// in_account` per member, which [`matched_members_for_roster`] then runs
/// again for its own loop — a small, cheap, indexed-lookup duplication
/// traded deliberately for "never call the directory when nothing needs
/// it", not an oversight.
///
/// Look up `reader` in `cache`; on a hit, clone and return the previously
/// recorded outcome (`Ok` or `Err` alike) without calling `fetch` at all. On
/// a miss, await `fetch`, record whatever it returns in `cache`, and return
/// it. The `bool` says which happened: `true` only on a genuine fetch, so a
/// caller that wants to log "the fetch failed" exactly once per pass (rather
/// than once per cache hit on an already-known failure) can gate on it.
///
/// Split out of [`auto_seat_space_members`] specifically so this caching
/// contract — CodeRabbit round 1 (#1005, Thread 23): *"Cache the result,
/// including an unavailable result, so each reader is fetched at most once
/// per pass"* — is unit-testable on its own, with a plain call-counting
/// closure, rather than needing a live People API call or a database the
/// way `auto_seat_space_members` itself does (this file has no HTTP-mock
/// harness for Google's APIs, unlike Slack's `SLACK_API_BASE` seam).
async fn directory_for_reader<F, Fut>(
    cache: &mut DomainDirectoryCache,
    reader: Uuid,
    fetch: F,
) -> (Result<HashMap<String, DirectoryEntry>, String>, bool)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<HashMap<String, DirectoryEntry>, String>>,
{
    if let Some(cached) = cache.get(&reader) {
        return (cached.clone(), false);
    }
    let fetched = fetch().await;
    cache.insert(reader, fetched.clone());
    (fetched, true)
}

/// `directory_cache` (CodeRabbit round 1, #1005, Thread 23): see
/// [`DomainDirectoryCache`]'s own doc comment. Looked up and, on a miss,
/// populated here (via [`directory_for_reader`]) — this is the ONLY place a
/// directory fetch happens for the roster-reconcile path, so it is also the
/// only place that needs to know about the cache at all.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn auto_seat_space_members(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    owner_user_id: Uuid,
    space_id: &str,
    team_id: Uuid,
    access_token: &str,
    directory_cache: &mut DomainDirectoryCache,
) -> Result<SeatingOutcome, String> {
    let roster = resolve_space_roster(http, access_token, space_id)
        .await
        .map_err(|e| format!("roster read for {space_id}: {e}"))?;

    // The connector_identities anchor. `None` for a legacy/malformed reader
    // row with no `provider_workspace_id` on file — identity writes are
    // skipped entirely for this pass rather than guessing an anchor; seating
    // via the sub-match path is unaffected, since it needs no
    // connector_identities row at all.
    let workspace_id = reader_workspace_id(control_pool, account_id, owner_user_id)
        .await
        .map_err(|e| format!("reader_workspace_id for {owner_user_id}: {e}"))?;

    // Pre-scan: does ANY eligible roster member fail the free, network-free
    // sub match? Skip the People API round trip entirely when the answer is
    // "no" (see this function's own doc comment). External members do not
    // force a fetch — their address is absent. A HUMAN with no affiliation
    // does, even when the sub already matches, because directory + domain
    // is the only admission rule for that row.
    let mut needs_directory = roster
        .members
        .iter()
        .any(|member| human_lacks_affiliation(&member.user_type, &member.affiliation));
    if workspace_id.is_some() {
        for member in &roster.members {
            if is_external_affiliation(&member.affiliation)
                || human_lacks_affiliation(&member.user_type, &member.affiliation)
            {
                continue;
            }
            match resolve_google_identity_in_account(control_pool, account_id, &member.sub).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    needs_directory = true;
                    break;
                }
                Err(e) => return Err(format!("identity resolution pre-scan for {}: {e}", member.sub)),
            }
        }
    }

    let anchor_domains = if roster
        .members
        .iter()
        .any(|member| human_lacks_affiliation(&member.user_type, &member.affiliation))
    {
        chat_invite_anchor_domain(control_pool, account_id, owner_user_id).await
    } else {
        Vec::new()
    };

    // `freshly_fetched` distinguishes an actual People API round trip from a
    // cache hit (Thread 23) so the warn below stays true to its own doc
    // comment — "fires once per pass" — rather than re-firing once per
    // space that happens to share this reader's already-cached failure.
    let (directory, freshly_fetched): (Result<HashMap<String, DirectoryEntry>, String>, bool) = if needs_directory {
        directory_for_reader(directory_cache, owner_user_id, || async {
            fetch_domain_directory(http, access_token).await.map_err(|e| e.to_string())
        })
        .await
    } else {
        (
            Err("domain directory not fetched this pass (either no workspace_id anchor, or every \
                 roster member already resolved via the sub match)"
                .to_string()),
            false,
        )
    };
    if freshly_fetched {
        if let Err(reason) = &directory {
            // `warn!`, not `info!` (coordinator review) — this is the ONE
            // log line that makes a wholesale directory failure diagnosable
            // without reading per-member noise: it fires once per pass, at
            // the fetch site (guarded on `freshly_fetched` so a cache hit on
            // an already-known failure never re-fires it — Thread 23), and
            // `reason` (from `fetch_domain_directory`'s own `Err` text)
            // already names the likely cause — a pre-widening connection
            // that has never re-consented to `directory.readonly`, or this
            // domain's Workspace admin not having enabled external contact/
            // profile sharing. The per-member `info!` inside
            // `matched_members_for_roster` stays, underneath this one, for
            // anyone who needs the per-sub detail.
            tracing::warn!(
                %account_id, %owner_user_id, space_id, connector_health = true,
                reason = %reason,
                "google_chat: domain-directory identity lookup unavailable this pass — every \
                 roster member the sub-match could not place is treated as UNAVAILABLE, never as \
                 a confirmed non-account, and no verified link is written"
            );
        }
    }

    let roster_outcome = matched_members_for_roster(
        control_pool,
        tenant_pool,
        account_id,
        workspace_id.as_deref(),
        &roster,
        directory.as_ref().map_err(|e| e.as_str()),
        &anchor_domains,
    )
    .await?;
    let matched = roster_outcome.matched;
    let source = crate::seats::google_chat_space_source(space_id);

    // Founding vs late-joiner (m365 Train 4.1's `history_from`): the FIRST
    // pass over a newly-routed space seats its founding roster with FULL
    // history (`None`) — §5.4's promise that auto-seated members read the
    // ocean back to its start, and withholding it would hide the backfill
    // this same routing action just triggered from the very people who wrote
    // it. Anyone discovered by a LATER sweep is a late joiner and gets
    // `Some(now)`.
    let history_from = crate::seats::founding_history_from(tenant_pool, account_id, team_id, &source)
        .await
        .map_err(|e| format!("founding_history_from for {source}: {e}"))?;

    let seated = crate::routes::slack_interactivity::seat_members_for_source(
        tenant_pool,
        account_id,
        team_id,
        &matched,
        &source,
        history_from,
    )
    .await;

    if seated > 0 && history_from.is_none() {
        if let Err(e) = crate::seats::record_source_founding(tenant_pool, account_id, team_id, &source).await {
            tracing::warn!(error = %e, %account_id, %team_id, space_id, "google_chat auto-seat: recording source founding failed — continuing");
        }
    }

    let total_humans = roster.total_humans();
    Ok(SeatingOutcome {
        total_humans,
        matched: matched.len(),
        seated,
        // `unmapped` = "asked, genuinely no account"; `unavailable` =
        // "could not even check" — kept as two separate counts rather than
        // one combined `total - matched`, per `SeatingOutcome::unmapped`'s
        // own doc comment (coordinator review).
        unmapped: unmapped_excluding_not_invited(
            total_humans,
            matched.len(),
            roster_outcome.unavailable,
            roster_outcome.not_invited,
        ),
        unavailable: roster_outcome.unavailable,
        group_memberships: roster.group_memberships,
        matched_user_ids: matched,
    })
}

/// Revoke what THIS space granted, for one departing member — the
/// `membership.v1.deleted` half of §3.4 (conformance test 7). Delegates to
/// [`crate::seats::revoke_seat_grant`]'s DELETE-first, count-the-dropped-rows
/// discipline, so a `preexisting` (hand-granted) seat is never ours to
/// revoke and a member seated by a SECOND routed space keeps their seat.
pub(crate) async fn revoke_space_seat(
    tenant_pool: &PgPool,
    account_id: Uuid,
    team_id: Uuid,
    user_id: Uuid,
    space_id: &str,
) -> Result<crate::seats::RevokeOutcome, sqlx::Error> {
    crate::seats::revoke_seat_grant(
        tenant_pool,
        account_id,
        team_id,
        user_id,
        &crate::seats::google_chat_space_source(space_id),
    )
    .await
}

/// The team whose ocean `space_id` routes into — **the GRANT side**, so it
/// requires the space to be currently `active`. Only a `mode = 'single'`
/// pick landing on a graph some team owns can grant seats: a fan-out pick
/// seats nobody (each person's content goes to their own ocean) and an
/// unrouted space has made no choice. Structurally
/// `seats::resolve_channel_team`, keyed on the space instead of a Slack
/// channel — plus the `active` predicate, which Slack has no analogue for.
pub(crate) async fn resolve_space_team_for_grant(
    tenant_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    resolve_space_team_inner(tenant_pool, account_id, space_id, true).await
}

/// The same lookup **for the REVOKE side, deliberately WITHOUT the `active`
/// predicate** (security review, M1 — reproduced).
///
/// Sharing one resolver between grant and revoke was a real, reachable
/// privilege leak. `record_removed_from_space` sets `active = false`, but the
/// per-member subscription is on `//chat.googleapis.com/spaces/-` and
/// `resolve_chat_space_account` returns accounts regardless of `active`, so a
/// `membership.v1.deleted` still ARRIVES after the app is removed from a
/// space. With the `active` predicate on the revoke path, that departure
/// resolved no team, `handle_chat_membership_event`'s `let … else` swallowed
/// it, and `revoke_space_seat` never ran — leaving the departed member a
/// seat on the destination TEAM ocean. And because auto-seating stamps
/// `history_from = NULL`, that surviving seat is **unbounded history of the
/// whole team ocean, including everything predating the space**.
///
/// The `active` guard is correct for granting (do not seat people into a
/// space we are no longer in) and wrong for revoking (always let go). Two
/// resolvers, so neither can be "simplified" into the other.
pub(crate) async fn resolve_space_team_for_revoke(
    tenant_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    resolve_space_team_inner(tenant_pool, account_id, space_id, false).await
}

async fn resolve_space_team_inner(
    tenant_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    require_active: bool,
) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("resolve_space_team scoped tx: {e}")))?;
    let team_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT t.id FROM google_chat_watched_spaces s \
         JOIN teams t ON t.graph_id = s.graph_id AND t.account_id = s.account_id \
         WHERE s.account_id = $1 AND s.space_id = $2 AND s.owner_user_id IS NULL \
           AND s.mode = 'single' AND s.graph_id IS NOT NULL \
           AND (NOT $3::boolean OR s.active = true) \
         LIMIT 1",
    )
    .bind(account_id)
    .bind(space_id)
    .bind(require_active)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();
    tx.rollback().await.ok();
    Ok(team_id)
}

// ---------------------------------------------------------------------------
// §3.5 / D8 — the designated reader, and failover re-election
// ---------------------------------------------------------------------------

/// A watched space plus everything an ingest pass needs, read in one scoped
/// hop. `mode IS NULL` (unrouted) rows are deliberately INCLUDED by
/// [`due_spaces_for_account`]'s caller only through this read's own filters
/// — see that function.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct IngestableSpace {
    pub(crate) space_id: String,
    pub(crate) mode: Option<String>,
    pub(crate) reader_user_id: Option<Uuid>,
    pub(crate) display_name: String,
    pub(crate) history_state: String,
    pub(crate) backfill_complete: bool,
}

/// Every ROUTED, ACTIVE container this account holds, least-recently-
/// ATTEMPTED first, bounded by `limit`.
///
/// # The rotation key is `last_resolved_at`, not `last_ingested_at`
/// # (review round 1, H3 — #722's fleet-starvation bug, avoided)
///
/// `last_ingested_at` only advances on SUCCESS. Ordering on it meant a
/// permanently-failing space kept sorting first forever: five of them pinned
/// every slot of an account's budget, ten across two accounts exhausted the
/// whole per-tick budget, and — because consumer discovery walks accounts in
/// a fixed order — every higher-sorting account was then never probed again.
/// Ten stuck spaces could silence the entire consumer fleet. #722 fixed the
/// identical shape in `workers::gmail_everything_mode` with a
/// `last_attempted_at` column stamped on EVERY outcome.
///
/// Chat needs no such column, and this PR takes no migration: `0198` already
/// carries `last_resolved_at NOT NULL DEFAULT now()`, whose own header
/// defines it as *"bumped on every destination RESOLUTION … success or
/// not-yet-routed alike"* — which is exactly what a sweep pass is.
/// [`mark_space_attempted`] stamps it at the START of every pass, so the
/// ordering below genuinely self-rotates through failures, not only
/// successes.
pub(crate) async fn mark_space_attempted(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Option<Uuid>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "mark_space_attempted").await?;
    sqlx::query(
        "UPDATE google_chat_watched_spaces SET last_resolved_at = now() \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NOT DISTINCT FROM $3",
    )
    .bind(account_id)
    .bind(space_id)
    .bind(owner_user_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("mark_space_attempted: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("mark_space_attempted commit: {e}")))?;
    Ok(())
}

/// See [`mark_space_attempted`] for why the ordering key is
/// `last_resolved_at`.
///
/// The three filters are each load-bearing, and none of them may be
/// "simplified" away (founder decision G4-Q6 / security-review L4):
///
/// * `active = true` — a space the Chat app was REMOVED from stops ingest
///   immediately (Slack #502's lesson, §3.1).
/// * `mode IS NOT NULL` — an UNROUTED space has no destination, so there is
///   nowhere to put its messages. It must neither ingest nor error-loop; it
///   simply is not due (§7 correction C).
/// * `owner_user_id IS NULL` — the CONTAINER archetype only. G4.5's mailbox
///   rows have their own, entirely separate opt-in path.
pub(crate) async fn due_spaces_for_account(
    content_pool: &PgPool,
    account_id: Uuid,
    limit: i64,
) -> Result<Vec<IngestableSpace>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "due_spaces_for_account").await?;
    let rows: Vec<IngestableSpace> = sqlx::query_as(
        "SELECT space_id, mode, reader_user_id, display_name, history_state, backfill_complete \
         FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND owner_user_id IS NULL AND active = true AND mode IS NOT NULL \
         ORDER BY last_resolved_at ASC, created_at ASC \
         LIMIT $2",
    )
    .bind(account_id)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("due_spaces_for_account: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows)
}

/// One routed, active container by id — the push path's own read (it knows
/// which space it wants; it must not walk the due list).
pub(crate) async fn ingestable_space(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
) -> Result<Option<IngestableSpace>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "ingestable_space").await?;
    let row: Option<IngestableSpace> = sqlx::query_as(
        "SELECT space_id, mode, reader_user_id, display_name, history_state, backfill_complete \
         FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NULL AND active = true \
           AND mode IS NOT NULL",
    )
    .bind(account_id)
    .bind(space_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("ingestable_space: {e}")))?;
    tx.rollback().await.ok();
    Ok(row)
}

// ---------------------------------------------------------------------------
// G4.5 — the MAILBOX archetype (`GROUP_CHAT`/`DIRECT_MESSAGE`), owner-scoped
// opt-in. See `docs/connectors/google-workspace.md` §9's "Deferred to
// G4.5/G4.6" note and this table's own migration header (`0198`'s archetype
// invariant) for the design this section implements.
//
// Unlike the container archetype (§3.1-§3.5 above), there is no separate
// "unrouted" state here: the CHECK constraint
// (`space_type <> 'SPACE' AND mode = 'single' AND owner_user_id IS NOT
// NULL`) requires a mailbox row to ALWAYS carry its destination the moment it
// exists — opting in and picking a destination are the same act, because the
// destination is never a choice (it is always the opting-in member's own
// private ocean, contract §7's Mailbox archetype). There is also no reader
// election/failover: the reader IS the owner, always — borrowing a
// colleague's token to read someone else's DM would defeat the entire
// "no fan-out ever" guarantee this archetype exists to provide.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct MailboxSpaceRow {
    #[allow(dead_code)]
    pub(crate) id: Uuid,
    pub(crate) space_id: String,
    pub(crate) space_type: String,
    pub(crate) graph_id: Uuid,
    pub(crate) display_name: String,
    pub(crate) history_state: String,
    pub(crate) active: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MailboxOptInError {
    /// `space.space_type == "SPACE"` — the container archetype's own picker
    /// (`routes::google_chat::set_space_destination`) is the correct route
    /// for that space, not this one. Refused defensively, mirroring
    /// [`record_added_to_space`]'s own symmetric guard (a `SPACE` row can
    /// never carry an `owner_user_id` — the table's own archetype CHECK
    /// would reject the write anyway; this returns a typed, user-facing
    /// error instead of a raw constraint-violation DB error).
    #[error("this is a shared space, not a direct message or group chat — use the space picker instead")]
    WrongArchetype,
    #[error("database error: {0}")]
    Db(String),
}

impl From<sqlx::Error> for MailboxOptInError {
    fn from(e: sqlx::Error) -> Self {
        MailboxOptInError::Db(e.to_string())
    }
}

/// Opt `owner_user_id` into `space` (a live `spaces.get` result, so
/// `space_type`/`display_name`/`history_state` are authoritative, never
/// caller-supplied) — landing exclusively in `graph_id` (resolved by the
/// caller via `routes::slack::resolve_primary_graph(.., IngestCaller::
/// Attributable(owner_user_id))`, same helper `routes::gmail_everything_mode`
/// uses for the identical "owner's own private ocean" resolution).
///
/// Idempotent: opting in twice (or re-opting-in after [`opt_out_of_space`])
/// reactivates the SAME row and refreshes display metadata, but — same as
/// [`pick_space_destination`]'s "never re-point" rule (I2: a destination is
/// fixed at authorisation time) — never rewrites `graph_id` on an existing
/// row. In practice this is moot (the destination is always this same
/// owner's one private ocean), but the row is written this way so the
/// invariant holds structurally rather than by coincidence.
pub(crate) async fn record_mailbox_optin(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    space: &ChatSpace,
    graph_id: Uuid,
) -> Result<MailboxSpaceRow, MailboxOptInError> {
    // Allowlist the two archetype values this row can legitimately carry
    // (matches the table's own archetype CHECK, migration 0198), rather than
    // denylisting `SPACE` alone (CodeRabbit round 1, PR #811) — an empty
    // string, `SPACE_TYPE_UNSPECIFIED`, or any future Chat API value Google
    // adds would otherwise fall through this guard undetected and only fail
    // downstream at the CHECK constraint, surfacing as an opaque, unlogged
    // HTTP 500 (`MailboxOptInError::Db`) instead of this typed, logged
    // refusal.
    if !matches!(space.space_type.as_str(), "GROUP_CHAT" | "DIRECT_MESSAGE") {
        tracing::warn!(
            space_id = %space.name, space_type = %space.space_type, %account_id, %owner_user_id,
            "google chat: mailbox opt-in refused for a non-mailbox archetype — the container \
             picker is the correct route for a SPACE, and any other value is unrecognised (see \
             connectors::google_chat::record_mailbox_optin)"
        );
        return Err(MailboxOptInError::WrongArchetype);
    }

    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| MailboxOptInError::Db(format!("record_mailbox_optin scoped tx: {e}")))?;
    let row: MailboxSpaceRow = sqlx::query_as(
        r#"
        INSERT INTO google_chat_watched_spaces
            (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id,
             display_name, history_state, active)
        VALUES ($1, $2, $3, 'single', $4, $5, $5, $6, $7, true)
        ON CONFLICT (account_id, space_id, owner_user_id) WHERE owner_user_id IS NOT NULL
        DO UPDATE SET
            active = true,
            display_name = EXCLUDED.display_name,
            history_state = EXCLUDED.history_state,
            last_resolved_at = now()
        RETURNING id, space_id, space_type, graph_id, display_name, history_state, active
        "#,
    )
    .bind(account_id)
    .bind(&space.name)
    .bind(&space.space_type)
    .bind(graph_id)
    .bind(owner_user_id)
    .bind(&space.display_name)
    .bind(&space.history_state)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await.map_err(|e| MailboxOptInError::Db(format!("record_mailbox_optin commit: {e}")))?;
    Ok(row)
}

/// Opt `owner_user_id` back OUT of `space_id` — `active = false`, same
/// "never delete, so a later re-opt-in resumes the same ocean" posture
/// [`record_removed_from_space`] documents for the container archetype.
/// Returns `true` iff a row existed to deactivate.
pub(crate) async fn opt_out_of_mailbox_space(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    space_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("opt_out_of_mailbox_space scoped tx: {e}")))?;
    let updated: Option<(Uuid,)> = sqlx::query_as(
        r#"
        UPDATE google_chat_watched_spaces
        SET active = false, last_resolved_at = now()
        WHERE account_id = $1 AND space_id = $2 AND owner_user_id = $3
        RETURNING id
        "#,
    )
    .bind(account_id)
    .bind(space_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(updated.is_some())
}

/// Every mailbox row `owner_user_id` personally opted into for this account
/// (routed or not, active or not) — the local half of the destination-listing
/// endpoint (`routes::google_chat::list_spaces`), analogous to
/// [`list_watched_spaces`] for the container archetype.
pub(crate) async fn list_watched_mailbox_spaces_for_owner(
    tenant_pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<HashMap<String, MailboxSpaceRow>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("list_watched_mailbox_spaces_for_owner scoped tx: {e}")))?;
    let rows: Vec<MailboxSpaceRow> = sqlx::query_as(
        r#"
        SELECT id, space_id, space_type, graph_id, display_name, history_state, active
        FROM google_chat_watched_spaces
        WHERE account_id = $1 AND owner_user_id = $2
        "#,
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|r| (r.space_id.clone(), r)).collect())
}

/// Every ACTIVE mailbox opt-in this account holds across EVERY owning
/// member, least-recently-attempted first, bounded by `limit` — the mailbox
/// analogue of [`due_spaces_for_account`], driving `workers::
/// google_chat_sweep`'s mailbox pass. There is no `mode IS NOT NULL` filter
/// (unlike the container query): the archetype CHECK constraint guarantees
/// every `owner_user_id IS NOT NULL` row is already `mode = 'single'` with a
/// non-NULL `graph_id`, so there is no "unrouted mailbox row" state to
/// exclude.
pub(crate) async fn due_mailbox_spaces_for_account(
    content_pool: &PgPool,
    account_id: Uuid,
    limit: i64,
) -> Result<Vec<(Uuid, IngestableSpace)>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "due_mailbox_spaces_for_account").await?;
    let rows: Vec<(Uuid, String, Option<String>, Option<Uuid>, String, String, bool)> = sqlx::query_as(
        "SELECT owner_user_id, space_id, mode, reader_user_id, display_name, history_state, backfill_complete \
         FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND owner_user_id IS NOT NULL AND active = true \
         ORDER BY last_resolved_at ASC, created_at ASC \
         LIMIT $2",
    )
    .bind(account_id)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("due_mailbox_spaces_for_account: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows
        .into_iter()
        .map(|(owner_user_id, space_id, mode, reader_user_id, display_name, history_state, backfill_complete)| {
            (
                owner_user_id,
                IngestableSpace {
                    space_id,
                    mode,
                    reader_user_id,
                    display_name,
                    history_state,
                    backfill_complete,
                },
            )
        })
        .collect())
}

/// One active mailbox opt-in by `(space_id, owner_user_id)` — the push
/// path's own read, mirroring [`ingestable_space`] for the container
/// archetype.
pub(crate) async fn ingestable_mailbox_space(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    owner_user_id: Uuid,
) -> Result<Option<IngestableSpace>, ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "ingestable_mailbox_space").await?;
    let row: Option<(String, Option<String>, Option<Uuid>, String, String, bool)> = sqlx::query_as(
        "SELECT space_id, mode, reader_user_id, display_name, history_state, backfill_complete \
         FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id = $3 AND active = true",
    )
    .bind(account_id)
    .bind(space_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("ingestable_mailbox_space: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(space_id, mode, reader_user_id, display_name, history_state, backfill_complete)| IngestableSpace {
        space_id,
        mode,
        reader_user_id,
        display_name,
        history_state,
        backfill_complete,
    }))
}

/// Resolve every `(account_id, owner_user_id)` pair that has opted a mailbox
/// (`GROUP_CHAT`/`DIRECT_MESSAGE`) `space_id` into their own private ocean —
/// the mailbox analogue of [`resolve_chat_space_accounts`], needed for the
/// SAME reason: a push notification arrives with no `account_id` in hand.
/// Deliberately not fail-closed on more than one match, for the SAME reason
/// [`resolve_chat_space_accounts`] is not: two different colleagues (even in
/// two different CoralSwarm accounts) may each independently opt the SAME
/// physical DM/group into their OWN private ocean, and a push for it must
/// reach every one of them, never just one.
pub(crate) async fn resolve_chat_mailbox_space_owners(
    control_pool: &PgPool,
    space_id: &str,
) -> Result<Vec<(Uuid, Uuid)>, ConnectorError> {
    if space_id.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT * FROM resolve_chat_mailbox_space_owners($1)")
        .bind(space_id)
        .fetch_all(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_chat_mailbox_space_owners: {e}")))?;
    Ok(rows)
}

/// Elect a designated reader for `space_id` — the member whose token the
/// SWEEP paths (backfill, catch-up, roster reconcile) use, so an N-member
/// space costs one API budget rather than N (§3.5).
///
/// **Failover is a RE-ELECTION, not a row deletion** (§1.8/§3.5). When the
/// designated reader disconnects, loses membership, or 403s, the row keeps
/// its destination and its cursors and simply acquires a different reader;
/// deleting it would silently discard a human's I2 destination pick and, on
/// a later re-add, restart the picker from scratch.
///
/// Candidates are CURRENT MEMBERS of this account with a live
/// `google_chat` connection, most-recently-refreshed first. `exclude` drops
/// readers already tried, so a failover never re-elects one that just
/// failed. Returns `None` when nobody is left — *"a space whose last
/// connected member disconnects becomes unreadable. That is not an error;
/// it is the correct end state"* (§3.5).
///
/// # The `account_members` gate is load-bearing (security review, H1)
///
/// Without it this read is `oauth_tokens` ALONE, and an offboarded
/// ex-member's Chat token outlives their membership:
/// `clerk_provision::remove_org_membership` deletes the `account_members`
/// row and **never touches `oauth_tokens`**, while
/// [`reelect_reader_for_departing_member`] only fires from an explicit Chat
/// DISCONNECT, never from a membership removal. So Alice is offboarded, her
/// token still sorts first on `updated_at DESC`, the next sweep elects her,
/// `refresh_if_needed` + `store_token_bundle` **re-persist a fresh token for
/// a non-member**, her credential reads every routed space, atoms land
/// `ingested_by = Alice`, and `ensure_subscription` recreates a Workspace
/// Events subscription in her name so Google keeps publishing her Chat
/// activity to our topic indefinitely. Reproduced against real Postgres.
///
/// Every other resolver in this module already carries this gate
/// ([`matched_members_for_roster`], [`resolve_chat_fanout_destinations`]);
/// this was the one that did not. A DB error on the gate **propagates** and
/// is never collapsed into "not a member" (§3.4's rule, and
/// `slack.rs:1461`-`:1468`'s incident).
///
/// `account_members` is a CONTROL-plane table with no `0083` policy, read on
/// the same control pool the token read uses.
pub(crate) async fn elect_space_reader(
    control_pool: &PgPool,
    account_id: Uuid,
    exclude: &[Uuid],
) -> Result<Option<Uuid>, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("elect_space_reader scoped tx: {e}")))?;
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT ot.owner_user_id FROM oauth_tokens ot \
         WHERE ot.account_id = $1 AND ot.provider = 'google_chat' \
           AND ot.owner_user_id IS NOT NULL \
           AND NOT (ot.owner_user_id = ANY($2)) \
           AND EXISTS ( \
                 SELECT 1 FROM account_members am \
                 WHERE am.account_id = $1 AND am.user_id = ot.owner_user_id \
               ) \
         ORDER BY ot.updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(exclude)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("elect_space_reader: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(u,)| u))
}

/// Is `user_id` a CURRENT member of `account_id`?
///
/// The membership half of [`elect_space_reader`]'s gate, split out so the
/// STORED reader can be held to the identical standard the newly-elected
/// one is (security review round 2, H1-follow-up).
///
/// # Why this exists as its own function
///
/// `elect_space_reader` refuses to hand a space to an ex-member, but the
/// sweep only reaches it when the row has NO stored reader or the stored
/// one's token refresh fails. `workers::google_chat_sweep::
/// reader_credentials` took `space.reader_user_id` as-is, so an offboarded
/// member whose refresh token still worked kept being used indefinitely —
/// their credential read the space every tick and `store_token_bundle`
/// re-persisted a freshly minted access token in their name. Offboarding
/// deletes the `account_members` row and never touches `oauth_tokens`
/// (`clerk_provision::remove_org_membership`), which is exactly the state
/// this predicate detects.
///
/// An `Err` means *we do not know* and callers must fail CLOSED (not use
/// the reader) rather than treat unknown as "member" — the same posture
/// `reader_credentials`' `load_creds_bg` arm already takes.
pub(crate) async fn is_current_account_member(
    control_pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("is_current_account_member scoped tx: {e}")))?;
    let found = crate::routes::slack::membership::is_current_member(&mut *tx, account_id, user_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("is_current_account_member: {e}")))?;
    tx.rollback().await.ok();
    Ok(found)
}

/// Persist a reader election. `None` clears the reader (nobody left) WITHOUT
/// deactivating or unrouting the row — see [`elect_space_reader`]'s doc.
pub(crate) async fn set_space_reader(
    content_pool: &PgPool,
    account_id: Uuid,
    space_id: &str,
    reader_user_id: Option<Uuid>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "set_space_reader").await?;
    sqlx::query(
        "UPDATE google_chat_watched_spaces SET reader_user_id = $3 \
         WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NULL",
    )
    .bind(account_id)
    .bind(space_id)
    .bind(reader_user_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("set_space_reader: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("set_space_reader commit: {e}")))?;
    Ok(())
}

/// Re-elect the reader on every container row this account holds that
/// currently names `departing_user_id` — the disconnect path's failover
/// (§1.8, §3.5). Returns `(re-elected, orphaned)`: `orphaned` rows kept
/// their destination and cursors but have nobody left to read them, which
/// [`connectors::google_disconnect`](crate::connectors::google_disconnect)
/// reports as a cause-neutral `connector_health` warning rather than a
/// terminal ledger row (§1.10 lesson 4 — the condition is ours to fix and
/// reversible).
/// Called from BOTH departure paths (security review, H1): an explicit
/// Chat disconnect (`routes::oauth::delete_connection` →
/// `connectors::google_disconnect::purge_google_connection`) **and** an org
/// membership removal (`clerk_provision::remove_org_membership`). Only the
/// first existed before; the second is how an offboarded ex-member's token
/// stayed elected, because that path deletes the `account_members` row and
/// never touches `oauth_tokens`.
pub(crate) async fn reelect_reader_for_departing_member(
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    departing_user_id: Uuid,
) -> Result<(usize, usize), ConnectorError> {
    let mut tx = scoped_tx(content_pool, account_id, "reelect_reader read").await?;
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT space_id FROM google_chat_watched_spaces \
         WHERE account_id = $1 AND owner_user_id IS NULL AND reader_user_id = $2",
    )
    .bind(account_id)
    .bind(departing_user_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("reelect_reader read: {e}")))?;
    tx.rollback().await.ok();

    // Resolved ONCE for the whole account — candidacy is an account-level
    // property (a live `google_chat` connection plus current membership),
    // not a per-space one.
    let replacement = elect_space_reader(control_pool, account_id, &[departing_user_id]).await?;
    let mut reelected = 0usize;
    let mut orphaned = 0usize;
    for (space_id,) in rows {
        set_space_reader(content_pool, account_id, &space_id, replacement).await?;
        if replacement.is_some() {
            reelected += 1;
        } else {
            orphaned += 1;
        }
    }
    Ok((reelected, orphaned))
}

// ---------------------------------------------------------------------------
// §3.2 — fan-out destinations (org plane only)
// ---------------------------------------------------------------------------

/// Every destination ocean a `mode='fanout'` space's messages should land
/// in, plus how many account members could NOT be mapped.
///
/// Structurally `connectors::slack::resolve_org_fanout_destinations`, ported
/// rather than reused because Slack's identity source is a SINGLE read
/// (`connector_identities`, keyed by one `workspace_id` parameter the
/// channel's own workspace supplies) where Chat's is the UNION of TWO
/// sources, keyed differently:
///
/// 1. The Google `sub` join against `oauth_tokens` (`provider LIKE
///    'google%'`) — a colleague who has connected ANY Google surface to
///    CoralSwarm themselves. Unchanged from before this function's
///    chat-per-user-setup update.
/// 2. **New, chat-per-user-setup PR (2026-09-20).** `connector_identities`
///    rows the roster-reconcile path
///    ([`match_and_persist_chat_verified_email`]) has already confirmed via
///    the Workspace domain directory — a colleague with a CoralSwarm
///    account who has NEVER connected Google at all. **This is the gap that
///    made the roster-matching work only partially useful**: without this
///    union, fan-out — the path that actually decides whose ocean content
///    lands in — could only ever reach people source (1) already covered,
///    so a colleague this PR can now PROVE the identity of via source (2)
///    still received nothing. Read the SAME way
///    `workers::google_chat_sweep::mention_directory` reads it: scoped by
///    [`account_chat_workspace_ids`] (never an unanchored `provider =
///    'google_chat'` read — see that function's own doc comment for why an
///    unanchored read is a cross-account leak on this un-RLS'd table), on
///    the CONTENT plane, `user_id IS NOT NULL` only. A prior version of this
///    doc comment said *"Chat never returns an email, so there is no
///    email-keyed directory row to intersect against"* — true when written,
///    false as of this PR; the row now exists, this function is what
///    intersects against it.
///
/// A DB error reading EITHER identity source **propagates** (`?`), exactly
/// like the pre-existing `oauth_tokens` read always did — never collapsed to
/// "this source found nobody," which would silently under-count `graphs`
/// and over-count `unmapped` for a reason that has nothing to do with
/// whether these people have accounts. There is no live network call on
/// this path at all (unlike the roster-reconcile path's own People API
/// fetch) — both identity reads are plain lookups of already-persisted
/// state, so there is no `Unavailable`-vs-`Unmatched` ambiguity to model
/// here the way [`ChatVerifiedIdentity`] models it for the write side; a
/// person simply absent from both sources is `unmapped`, correctly.
///
/// **Read order is load-bearing and must not be "tidied" back into
/// declaration order** (§1.10 lesson 2): identities FIRST (BOTH sources),
/// members LAST. A member removed from `account_members` during the gap is
/// absent from the fresher member read, so the intersection drops them —
/// correct, they lose access immediately. The reverse order would land a
/// message in a REMOVED member's private ocean: *"an access-control
/// failure, not a stale-read annoyance."* The residual race is benign and
/// deliberate — a member ADDED during the gap misses this one pass and
/// picks up the next. *"Losing one message beats leaking one."*
///
/// **`unmapped` is DERIVED** — `total_members.saturating_sub(mapped
/// members.len())`, where the mapped set is of MEMBERS (a `HashSet<Uuid>`
/// deduplicating BOTH identity sources before it is ever compared against
/// `graphs`), not of graphs (`slack.rs:561`-`:570`'s bug class: an
/// independent `count(*)` produced a `connector_health` warning claiming N
/// members were unmapped when they had just been successfully seated).
///
/// `oauth_tokens`/`account_members` reads are CONTROL-plane for Chat;
/// `connector_identities`/`graphs` are content-plane (same split
/// [`match_and_persist_chat_verified_email`] and [`mention_directory`] already
/// use for that table). On the personal plane every pool is the same
/// database, so this is byte-identical there — and unreachable anyway,
/// since [`pick_space_destination`] refuses `FanOut` off the Org plane.
pub(crate) async fn resolve_chat_fanout_destinations(
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
) -> Result<crate::connectors::slack::FanOut, ConnectorError> {
    // IDENTITIES first (see this function's doc), source 1: the Google
    // `sub` join. `provider LIKE 'google%'` for §3.3's reason — a colleague
    // who connected Gmail but not yet Chat is still a person we can
    // identify with certainty, and refusing to route to their own ocean
    // would be I4 read backwards. Identification only; no credential
    // column is selected.
    let sub_matched_user_ids: Vec<Uuid> = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
            .await
            .map_err(|e| ConnectorError::Internal(format!("chat fan-out identity scope: {e}")))?;
        let rows: Vec<(Uuid,)> = sqlx::query_as(
            "SELECT DISTINCT owner_user_id FROM oauth_tokens \
             WHERE account_id = $1 AND provider LIKE 'google%' AND owner_user_id IS NOT NULL \
               AND provider_user_id <> ''",
        )
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("chat fan-out identities: {e}")))?;
        tx.rollback().await.ok();
        rows.into_iter().map(|(u,)| u).collect()
    };

    // IDENTITIES, source 2 (chat-per-user-setup PR): the domain-directory
    // verified-email links the roster path has already confirmed and
    // persisted — see this function's own doc comment for why this is a
    // union with source 1 above, not a replacement for it. `account_
    // chat_workspace_ids` is empty for an account that has never connected
    // ANY Chat workspace, in which case there is nothing to read — same
    // short-circuit `mention_directory` applies for the identical reason.
    let directory_linked_user_ids: Vec<Uuid> = {
        let workspace_ids = account_chat_workspace_ids(control_pool, account_id).await?;
        if workspace_ids.is_empty() {
            Vec::new()
        } else {
            let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(content_pool, account_id)
                .await
                .map_err(|e| ConnectorError::Internal(format!("chat fan-out directory-link scope: {e}")))?;
            let rows: Vec<(Uuid,)> = sqlx::query_as(
                "SELECT DISTINCT user_id FROM connector_identities \
                 WHERE provider = 'google_chat' AND workspace_id = ANY($1) AND user_id IS NOT NULL",
            )
            .bind(&workspace_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| ConnectorError::Internal(format!("chat fan-out directory-linked identities: {e}")))?;
            tx.rollback().await.ok();
            rows.into_iter().map(|(u,)| u).collect()
        }
    };

    // MEMBERS last.
    let member_user_ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT user_id FROM account_members WHERE account_id = $1")
            .bind(account_id)
            .fetch_all(control_pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("chat fan-out members: {e}")))?;
    let total_members = member_user_ids.len();
    let member_set: HashSet<Uuid> = member_user_ids.into_iter().collect();

    // UNION the two identity sources (deduplicated by MEMBER, per this
    // function's own doc comment on why `unmapped` must never see a member
    // counted twice just because both sources happened to name them), THEN
    // intersect with current membership.
    let identity_user_ids: HashSet<Uuid> =
        sub_matched_user_ids.into_iter().chain(directory_linked_user_ids).collect();
    let confirmed: Vec<Uuid> = identity_user_ids
        .into_iter()
        .filter(|u| member_set.contains(u))
        .collect();

    // One batched `graphs` read rather than a `resolve_primary_graph` round
    // trip per member — the predicate is `routes::slack::primary_ocean::
    // MEMBER`, the one `resolve_org_member_primary_graph` binds (account,
    // owner, `kind='member'`, `archived_at IS NULL`, `is_primary`), because
    // widening ANY of those is the cross-member leak that function's own
    // doc exists to warn about.
    let batched: HashMap<Uuid, Uuid> = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(content_pool, account_id)
            .await
            .map_err(|e| ConnectorError::Internal(format!("chat fan-out graph scope: {e}")))?;
        let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(&format!(
            "SELECT owner_user_id, id FROM graphs WHERE account_id = $1 AND owner_user_id = ANY($2) AND {}",
            crate::routes::slack::primary_ocean::MEMBER
        ))
        .bind(account_id)
        .bind(&confirmed)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("chat fan-out graphs: {e}")))?;
        tx.rollback().await.ok();
        rows.into_iter().collect()
    };

    let mut graphs: Vec<Uuid> = Vec::new();
    let mut mapped_members: HashSet<Uuid> = HashSet::new();
    for user_id in confirmed {
        // Identity confirmed but no member ocean provisioned yet: counted as
        // unmapped, never fabricated.
        if let Some(&graph_id) = batched.get(&user_id) {
            if mapped_members.insert(user_id) {
                graphs.push(graph_id);
            }
        }
    }
    Ok(crate::connectors::slack::FanOut {
        graphs,
        unmapped_members: total_members.saturating_sub(mapped_members.len()),
        members: member_set,
    })
}

// ---------------------------------------------------------------------------
// §3.9 / D10 — Path 1: the multi-destination write
// ---------------------------------------------------------------------------

/// What one [`ingest_space_items`] call accomplished. The caller commits a
/// cursor ONLY when `destinations_failed == 0 && items_failed == 0` — the
/// commit-after-ingest discipline, expressed over Path 1's counters rather
/// than Path 2's HTTP statuses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WriteOutcome {
    pub(crate) atoms_written: usize,
    pub(crate) destinations_ok: usize,
    pub(crate) destinations_failed: usize,
    pub(crate) items_failed: usize,
}

impl WriteOutcome {
    pub(crate) fn fully_succeeded(&self) -> bool {
        self.destinations_failed == 0 && self.items_failed == 0
    }
}

/// Write every item to every destination graph — **Path 1** (§3.9/D10), the
/// Slack shape (`routes::slack_events::ingest_slack_message`'s
/// per-destination loop, with `workers::huddle::ingest_huddle_notes` as the
/// fully-worked multi-graph reference).
///
/// # Why not Path 2 (`ingest_atom` in-process)
///
/// A space's destination may be a TEAM ocean, and
/// `ingest::is_own_writable_ocean` refuses `kind='org'`/`'team'` by
/// design — *"a shared ocean is promotion-fed"*. So `ingest_atom` is
/// structurally unavailable for the container archetype. **On the personal
/// plane a space's destination is `kind='personal'`, which DOES pass that
/// gate**, so shipping the Path-2 shape here would work today and break at
/// G4.6 — design doc §3.8/line 524 forbids exactly that, because the org
/// lift would then be a rewrite rather than a gate flip. G4.5's DM/group
/// path is Path 2 and stays Path 2; only this one is hand-rolled.
///
/// # The four lessons this loop is required to keep (§1.10)
///
/// * **Lesson 3, no bare `?` inside the per-destination loop.** Each
///   iteration is its own fallible `async` block, so a transient failure on
///   destination 2 of N cannot silently skip 3..N. That bug was live in
///   `slack_events.rs` and *"unreachable while fan-out returned ≤1 graph,
///   which is exactly why no test caught it"*.
/// * **Lesson 4, an infrastructure gap never writes a terminal row.** A DEK
///   or blob-key failure returns without committing; nothing here writes an
///   `ON CONFLICT DO NOTHING` marker that a later fix could not overwrite.
/// * Per-destination independence is reported honestly
///   (`destinations_ok`/`destinations_failed`), never collapsed into one
///   ambiguous count.
/// * BLOCKER 2's orphan cleanup: a failed body upload fails the item, so
///   nothing bodiless is ever returned as "ingested".
///
/// Every destination is an ocean of `grant`, the space's route; the ingest
/// door re-checks that route in each write's transaction.
pub(crate) async fn ingest_space_items(
    state: &std::sync::Arc<crate::AppState>,
    tenant: &crate::tenancy::TenantId,
    content_pool: &PgPool,
    grant: &crate::ingest::OrgSharedGrant,
    space_display_name: &str,
    ingested_by: Option<Uuid>,
    items: &[NormalizedItem],
) -> WriteOutcome {
    use coralswarm_core::chunker::chunk;
    use sha2::Digest as _;

    let account_id = grant.account_id();
    let mut outcome = WriteOutcome::default();
    if items.is_empty() || grant.oceans().is_empty() {
        return outcome;
    }
    // Per-item failures counted from INSIDE the per-destination async block
    // (review round 1, M1). The block returns `Result<usize, _>`, so a
    // failure that must not abort the destination — a blob upload that lost
    // its atom — has nowhere else to report itself.
    let failed_items = std::sync::atomic::AtomicUsize::new(0);

    // One DEK per account, resolved once. A null-key "encryption" would be a
    // security hole, so a failure here bails the whole write rather than
    // falling through to a placeholder — `slack_events`' own posture.
    let dek: [u8; 32] =
        match crate::crypto::resolve_dek_cached_for_tenant(tenant, content_pool, account_id, &state.dek_cache).await {
            Ok(Some(d)) => d,
            Ok(None) => {
                tracing::warn!(%account_id, "google_chat: account has no DEK — skipping ingest");
                outcome.items_failed = items.len();
                return outcome;
            }
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "google_chat: DEK resolution failed — skipping ingest");
                outcome.items_failed = items.len();
                return outcome;
            }
        };

    for &graph_id in grant.oceans() {
        let destination: Result<usize, Box<dyn std::error::Error + Send + Sync>> = async {
            let mut atoms_here = 0usize;
            for item in items {
                let chunks = chunk(&item.content, crate::ingest::capture::chunk_strategy(&item.source_type, None, None));
                if chunks.is_empty() {
                    continue;
                }
                let when = item.timestamp.format("%Y-%m-%d %H:%M UTC");
                let author = item.author_name.clone().unwrap_or_else(|| "Unknown author".to_string());
                let where_ = if space_display_name.trim().is_empty() {
                    item.channel_id.clone().unwrap_or_default()
                } else {
                    space_display_name.to_string()
                };
                let context_header = format!("{author} in {where_} · {when}");
                let content_hash = hex::encode(sha2::Sha256::digest(item.content.as_bytes()));

                let write:Result<usize,Box<dyn std::error::Error+Send+Sync>>=async {
                    let namespace=item.channel_id.as_ref().map(|space|format!("google_chat:space:{space}"))
                        .ok_or("google_chat source is missing its space identity")?;
                    let metadata=serde_json::json!({"channel_id":item.channel_id,"channel_name":space_display_name,
                        "author_id":item.author_id,"author_name":item.author_name});
                    let origin = crate::source_origin::SourceOrigin::native("google_chat","message","google_chat_space_capture")?
                        .with_container("space",item.channel_id.as_deref().ok_or("missing space")?,&namespace,
                            (!space_display_name.trim().is_empty()).then_some(space_display_name))?;
                    // Version, chunks, bodies, reconcile/links and the embed
                    // job in one transaction, through the ingest door. The
                    // "who in where · when" header stays inline with no
                    // `header_len` (historical shape: the chunk hash covers
                    // it); the title and excerpt come from the message body.
                    // A failed upload fails the item.
                    let prepared=crate::ingest::capture::Prepared::new(&item.source_id,&item.source_type,&item.content,
                        &chunks,&crate::ingest::capture::Header::Inline(context_header.clone()),None);
                    let ingested=crate::ingest::submit(crate::ingest::IngestRequest {
                        origin:Some(&origin),
                        ..crate::ingest::IngestRequest::new(crate::ingest::Destination::OrgShared(crate::ingest::Routed {
                            ocean:graph_id, grant,
                            writer:crate::ingest::SystemWriter {pool:content_pool,tenant,dek:&dek,r2:state.r2.as_deref()},
                            source:crate::source_evidence::CaptureSource {
                                source_type:&item.source_type,provider_namespace:&namespace,source_id:&item.source_id,
                                normalized_text:&item.content,representation:"normalized_text",metadata:&metadata,
                                occurred_at:Some(item.timestamp),participants:&item.participants,
                                ingested_by,scope:"account",duration_ms:None,segments:None,
                            },
                            chunks:&prepared,
                            atoms:crate::ingest::capture::AtomFields {
                                scope:"account",
                                channel_id:item.channel_id.as_deref(),channel_name:Some(space_display_name),
                                author_id:item.author_id.as_deref(),author_name:item.author_name.as_deref(),
                                ..crate::ingest::capture::AtomFields::new(&item.source_type,&item.participants,item.timestamp,ingested_by)
                            },
                            content_hash:&content_hash,
                            sync_ops:None,
                        }))
                    }).await;
                    match ingested {
                        Ok(ingested)=>Ok(ingested.body.atom_ids.len()),
                        Err(refused)=>Err(refused.into()),
                    }
                }.await;
                match write {
                    Ok(count)=>atoms_here+=count,
                    Err(error)=>{
                        failed_items.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                        return Err(error);
                    }
                }

            }
            Ok(atoms_here)
        }
        .await;

        match destination {
            Ok(n) => {
                outcome.destinations_ok += 1;
                outcome.atoms_written += n;
            }
            Err(e) => {
                outcome.destinations_failed += 1;
                tracing::warn!(
                    error = %e, %graph_id, %account_id,
                    "google_chat ingest: destination failed — continuing with the remaining \
                     destinations (each is independently best-effort; a failure here must not \
                     skip the rest of the fan-out)"
                );
            }
        }
    }
    outcome.items_failed += failed_items.load(std::sync::atomic::Ordering::Relaxed);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn added_to_space_for_an_org_writes_the_tenant_plane() {
        assert_eq!(added_to_space_account_plane(true), AccountPlane::Org);
        assert_eq!(added_to_space_account_plane(false), AccountPlane::Personal);
    }

    #[test]
    fn bare_sub_strips_the_users_prefix() {
        assert_eq!(bare_sub("users/12345"), "12345");
        // Malformed/foreign input is returned unchanged, never panics.
        assert_eq!(bare_sub("12345"), "12345");
        assert_eq!(bare_sub(""), "");
    }

    #[test]
    fn seating_disclosure_default_is_unavailable_not_zero() {
        // Pins the "an unknown roster is never treated as an empty one"
        // rule at the type level: the zero-value default must read as
        // "unavailable", not "verified empty".
        let d = SeatingDisclosure::default();
        assert!(!d.available);
        assert_eq!(d.total_humans, 0);
        assert_eq!(d.unmapped, 0);
    }

    #[test]
    fn unmapped_is_derived_never_independent() {
        // The `slack.rs:593-596` bug class (design doc §3.4): `unmapped`
        // must always equal `total.saturating_sub(matched)`, never a
        // separately-tracked count that could drift from it.
        let d = SeatingDisclosure {
            total_humans: 5,
            matched: 3,
            unmapped: 5usize.saturating_sub(3),
            group_memberships: 1,
            available: true,
        };
        assert_eq!(d.unmapped, 2);

        // saturating_sub, not a bare subtraction: matched can never
        // legitimately exceed total, but the arithmetic itself must not be
        // able to underflow-panic if it ever did.
        assert_eq!(5usize.saturating_sub(9), 0);
    }

    // ── Fixture-driven wire-shape checks ────────────────────────────────────

    #[test]
    fn spaces_list_response_parses_the_g4_2_fixture() {
        const FIXTURE: &str = include_str!("../../tests/fixtures/google_chat/spaces_list.json");
        let parsed: SpacesListResponse = serde_json::from_str(FIXTURE).unwrap();
        assert_eq!(parsed.spaces.len(), 5);
        assert_eq!(parsed.spaces[0].space_type, "SPACE");
    }

    #[test]
    fn members_list_response_parses_the_g4_2_fixture() {
        const FIXTURE: &str = include_str!("../../tests/fixtures/google_chat/members_list.json");
        let parsed: MembersListResponse = serde_json::from_str(FIXTURE).unwrap();
        assert_eq!(parsed.memberships.len(), 7);
        let joined_humans = parsed
            .memberships
            .iter()
            .filter(|m| m.state == "JOINED" && m.member.as_ref().is_some_and(|u| u.user_type == "HUMAN"))
            .count();
        assert_eq!(joined_humans, 4, "4 JOINED humans — the INVITED one and the BOT are excluded");
        let groups = parsed.memberships.iter().filter(|m| m.group_member.is_some()).count();
        assert_eq!(groups, 1);
    }

    #[test]
    fn pubsub_push_chat_app_added_to_space_fixture_decodes_to_added_to_space() {
        // Not this module's job to decode the Pub/Sub envelope itself
        // (`routes::google_push` owns that) — but the interaction-event
        // BODY shape this module's `ChatSpace`/identity plumbing consumes
        // is exactly what `routes::google_push::ChatInteractionEvent`
        // decodes from `message.data`; this pins that the fixture really is
        // an `ADDED_TO_SPACE` event carrying a `SPACE`-typed space, so a
        // change to the fixture that silently stopped being one would fail
        // here rather than only in `routes::google_push`'s own tests.
        use base64::Engine as _;
        const ENVELOPE: &str =
            include_str!("../../tests/fixtures/google_chat/pubsub_push_chat_app_added_to_space.json");
        let v: serde_json::Value = serde_json::from_str(ENVELOPE).unwrap();
        let data = v["message"]["data"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD.decode(data).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(body["type"], "ADDED_TO_SPACE");
        assert_eq!(body["space"]["spaceType"], "SPACE");
    }

    // ── DB-level tests (pg-gated) ────────────────────────────────────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
    //     cargo test -p coralswarm-backend --lib connectors::google_chat::tests

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }
    /// [`seed_user`] with a CALLER-CHOSEN email — for the domain-directory
    /// verified-email matcher tests, which need to control the exact address
    /// on both sides of the comparison (the `users` row and the directory
    /// entry) rather than accept `seed_user`'s random one.
    async fn seed_user_with_email(pool: &PgPool, email: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn seed_account(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn seed_oauth_token(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid, provider: &str, sub: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
                (account_id, provider, scope, owner_user_id, provider_user_id, access_token, refresh_token) \
             VALUES ($1, $2, 'test-scope', $3, $4, 'enc-access', 'enc-refresh')",
        )
        .bind(account_id)
        .bind(provider)
        .bind(owner_user_id)
        .bind(sub)
        .execute(pool)
        .await
        .unwrap();
    }
    #[test]
    fn classify_app_membership_table() {
        let joined = r#"{"name":"spaces/X/members/app","state":"JOINED","member":{"name":"users/app","type":"BOT"}}"#;
        assert_eq!(classify_app_membership(200, joined), AppMembership::Joined);
        assert_eq!(
            classify_app_membership(200, r#"{"state":"INVITED"}"#),
            AppMembership::Absent
        );
        assert_eq!(classify_app_membership(404, "{}"), AppMembership::Absent);
        assert_eq!(classify_app_membership(403, "{}"), AppMembership::Unknown);
        assert_eq!(classify_app_membership(500, ""), AppMembership::Unknown);
        assert_eq!(classify_app_membership(200, "not json"), AppMembership::Unknown);
    }

    #[test]
    fn spaces_needing_app_probe_skips_active_rows_and_non_spaces() {
        let mut dm = test_space("spaces/DM");
        dm.space_type = "DIRECT_MESSAGE".to_string();
        let live = vec![test_space("spaces/NEW"), test_space("spaces/ACTIVE"), test_space("spaces/OFF"), dm];
        let row = |id: &str, active: bool| WatchedSpaceRow {
            id: Uuid::nil(),
            account_id: Uuid::nil(),
            space_id: id.to_string(),
            space_type: "SPACE".to_string(),
            mode: None,
            graph_id: None,
            reader_user_id: None,
            display_name: String::new(),
            history_state: String::new(),
            active,
        };
        let mut watched = HashMap::new();
        watched.insert("spaces/ACTIVE".to_string(), row("spaces/ACTIVE", true));
        watched.insert("spaces/OFF".to_string(), row("spaces/OFF", false));
        let names: Vec<&str> = spaces_needing_app_probe(&live, &watched, 0).iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["spaces/NEW", "spaces/OFF"]);
    }

    #[test]
    fn spaces_needing_app_probe_is_capped() {
        let live: Vec<ChatSpace> = (0..APP_PROBE_CAP + 10).map(|i| test_space(&format!("spaces/S{i}"))).collect();
        assert_eq!(spaces_needing_app_probe(&live, &HashMap::new(), 0).len(), APP_PROBE_CAP);
    }

    #[test]
    fn spaces_needing_app_probe_rotates_past_the_cap() {
        let n = APP_PROBE_CAP + 10;
        let live: Vec<ChatSpace> = (0..n).map(|i| test_space(&format!("spaces/S{i}"))).collect();
        let last = format!("spaces/S{}", n - 1);
        let mut cursor = 0;
        let mut seen = HashSet::new();
        for _ in 0..3 {
            let sel = spaces_needing_app_probe(&live, &HashMap::new(), cursor);
            assert!(sel.len() <= APP_PROBE_CAP);
            cursor += sel.len();
            seen.extend(sel.iter().map(|s| s.name.clone()));
        }
        assert!(seen.contains(&last), "space beyond the cap was never probed");
        assert_eq!(seen.len(), n);
    }

    fn test_space(name: &str) -> ChatSpace {
        ChatSpace {
            name: name.to_string(),
            space_type: "SPACE".to_string(),
            display_name: "eng-platform".to_string(),
            space_threading_state: "THREADED_MESSAGES".to_string(),
            history_state: "HISTORY_ON".to_string(),
            single_user_bot_dm: false,
            membership_count: None,
        }
    }

    #[tokio::test]
    async fn resolve_google_identity_by_sub_resolves_unambiguous_and_fails_closed_on_collision_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resolve_google_identity_by_sub_resolves_unambiguous_and_fails_closed_on_collision_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_identity_by_sub";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Empty sub: never resolves, never even queries.
        assert_eq!(resolve_google_identity_by_sub(&pool, "").await.unwrap(), None);
        // Unknown sub: no rows.
        assert_eq!(resolve_google_identity_by_sub(&pool, "no-such-sub").await.unwrap(), None);

        // Unambiguous: one account, connected via google_chat.
        let owner_a = seed_user(&pool).await;
        let account_a = seed_account(&pool, owner_a).await;
        seed_oauth_token(&pool, account_a, owner_a, "google_chat", "sub-alice").await;
        assert_eq!(
            resolve_google_identity_by_sub(&pool, "sub-alice").await.unwrap(),
            Some((account_a, owner_a))
        );

        // Identification-only, not credential-exact: a Gmail-only
        // connection for the SAME sub under the SAME account still
        // resolves — `provider LIKE 'google%'`, not `= 'google_chat'`
        // (§3.3's argument).
        let owner_b = seed_user(&pool).await;
        let account_b = seed_account(&pool, owner_b).await;
        seed_oauth_token(&pool, account_b, owner_b, "google_gmail", "sub-bob").await;
        assert_eq!(
            resolve_google_identity_by_sub(&pool, "sub-bob").await.unwrap(),
            Some((account_b, owner_b))
        );

        // Ambiguous: the SAME sub connected under TWO different accounts —
        // fails closed to None, never an arbitrary pick.
        seed_oauth_token(&pool, account_b, owner_b, "google_chat", "sub-alice").await;
        assert_eq!(
            resolve_google_identity_by_sub(&pool, "sub-alice").await.unwrap(),
            None,
            "a sub connected under two different accounts must resolve to NOTHING, not an \
             arbitrary pick"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn resolve_google_identity_by_sub_function_source_never_selects_a_token_column_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resolve_google_identity_by_sub_function_source_never_selects_a_token_column_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_identity_by_sub_source";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let src: String = sqlx::query_scalar(
            "SELECT prosrc FROM pg_proc WHERE proname = 'resolve_google_identity_by_sub'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        for forbidden in ["access_token", "refresh_token"] {
            assert!(
                !src.contains(forbidden),
                "resolve_google_identity_by_sub's SQL body must never select a credential \
                 column ({forbidden} found) — it identifies, it never loads a token"
            );
        }

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn added_to_space_creates_unrouted_row_and_redundant_re_add_while_active_preserves_the_pick_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping added_to_space_creates_unrouted_row_and_redundant_re_add_while_active_preserves_the_pick_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_added_to_space";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();

        let owner = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        let graph_id = seed_tenant_graph(&pool, account_id).await;

        // ADDED_TO_SPACE: creates an unrouted row.
        let row = record_added_to_space(&pool, &pool, AccountPlane::Personal, account_id, Some(owner), &test_space("spaces/AAA"))
            .await
            .unwrap()
            .expect("SPACE archetype always returns a row");
        assert_eq!(row.mode, None);
        assert_eq!(row.graph_id, None);
        assert!(row.active);

        // A human picks a destination.
        let picked = pick_space_destination(
            &pool,
            &pool,
            AccountPlane::Personal,
            account_id,
            "spaces/AAA",
            PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        assert!(picked.newly_routed);
        assert_eq!(picked.row.mode.as_deref(), Some("single"));
        assert_eq!(picked.row.graph_id, Some(graph_id));

        // A second pick attempt is refused-as-already-routed, not silently
        // re-pointed (I2).
        let other_graph = seed_tenant_graph(&pool, account_id).await;
        let second_pick = pick_space_destination(
            &pool,
            &pool,
            AccountPlane::Personal,
            account_id,
            "spaces/AAA",
            PickedDestination::Single(other_graph),
        )
        .await
        .unwrap();
        assert!(!second_pick.newly_routed);
        assert_eq!(
            second_pick.row.graph_id,
            Some(graph_id),
            "a second pick must never re-point an already-routed space"
        );

        // A REDUNDANT ADDED_TO_SPACE while the row is STILL active (no
        // removal happened) — the "duplicate delivery" case (founder
        // decision G4-Q6): the existing pick is preserved exactly.
        let redundant = record_added_to_space(&pool, &pool, AccountPlane::Personal, account_id, Some(owner), &test_space("spaces/AAA"))
            .await
            .unwrap()
            .unwrap();
        assert!(redundant.active);
        assert_eq!(redundant.mode.as_deref(), Some("single"), "a redundant re-add while still active must NOT reset an existing pick");
        assert_eq!(redundant.graph_id, Some(graph_id));

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn full_lifecycle_add_route_third_party_removal_re_add_resets_to_unrouted_pg() {
        // Founder decision G4-Q6 (2026-08-25), pinned end to end: add →
        // unrouted → routed → removed (by a THIRD PARTY who never
        // connected CoralSwarm — H1's scenario, resolved by SPACE via
        // `resolve_chat_space_accounts`, never by the remover's identity)
        // → re-add → UNROUTED AGAIN, never a silent resurrection of the
        // prior destination. Also exercises L4: while deactivated, a pick
        // attempt is refused.
        let Some(base) = base_url() else {
            eprintln!("skipping full_lifecycle_add_route_third_party_removal_re_add_resets_to_unrouted_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_full_lifecycle";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_id = seed_account(&pool, seed_user(&pool).await).await;
        let graph_id = seed_tenant_graph(&pool, account_id).await;

        // add → unrouted
        let added = record_added_to_space(&pool, &pool, AccountPlane::Personal, account_id, None, &test_space("spaces/lifecycle"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(added.mode, None);
        assert_eq!(added.graph_id, None);

        // unrouted → routed
        let picked = pick_space_destination(
            &pool, &pool, AccountPlane::Personal, account_id, "spaces/lifecycle", PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        assert_eq!(picked.row.mode.as_deref(), Some("single"));
        assert_eq!(picked.row.graph_id, Some(graph_id));

        // routed → removed, BY A THIRD PARTY: resolved entirely by space,
        // with no identity for the remover anywhere in this test — the
        // exact shape `routes::google_push::handle_chat_removed_from_space`
        // now uses.
        let accounts = resolve_chat_space_accounts(&pool, "spaces/lifecycle").await.unwrap();
        assert_eq!(accounts, vec![account_id]);
        for acct in accounts {
            assert!(record_removed_from_space(&pool, acct, "spaces/lifecycle").await.unwrap());
        }
        let after_removal = list_watched_spaces(&pool, account_id).await.unwrap();
        assert!(!after_removal["spaces/lifecycle"].active);
        // L4: while removed, a pick attempt is refused.
        let refused = pick_space_destination(
            &pool, &pool, AccountPlane::Personal, account_id, "spaces/lifecycle", PickedDestination::Single(graph_id),
        )
        .await;
        assert!(matches!(refused, Err(PickError::NotConnected)));

        // removed → re-add: resets to UNROUTED, never resurrects the prior
        // destination — the roster may have turned over completely.
        let readded = record_added_to_space(&pool, &pool, AccountPlane::Personal, account_id, None, &test_space("spaces/lifecycle"))
            .await
            .unwrap()
            .unwrap();
        assert!(readded.active, "re-add must reactivate the row");
        assert_eq!(readded.mode, None, "re-add after removal must reset mode to unrouted, never resurrect the prior pick");
        assert_eq!(readded.graph_id, None, "re-add after removal must clear graph_id — no ingest destination survives a removal");

        // And the row genuinely requires a fresh pick again — it isn't
        // just cosmetically NULL while still secretly routable.
        let repicked = pick_space_destination(
            &pool, &pool, AccountPlane::Personal, account_id, "spaces/lifecycle", PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        assert!(repicked.newly_routed, "a fresh pick after re-add must be accepted as NEWLY routed");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── Security review round 2: M3 (account-scoped identity), H1
    // (space-keyed removal fan-out), L4 (inactive space refuses a pick) ──

    #[tokio::test]
    async fn resolve_google_identity_in_account_is_account_scoped_and_deterministic_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resolve_google_identity_in_account_is_account_scoped_and_deterministic_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_identity_in_account";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner_a = seed_user(&pool).await;
        let account_a = seed_account(&pool, owner_a).await;
        seed_oauth_token(&pool, account_a, owner_a, "google_chat", "shared-sub").await;

        let owner_b = seed_user(&pool).await;
        let account_b = seed_account(&pool, owner_b).await;

        // Account-scoped (M3): resolves under its OWN account.
        assert_eq!(
            resolve_google_identity_in_account(&pool, account_a, "shared-sub").await.unwrap(),
            Some(owner_a)
        );
        // The SAME sub, queried under a DIFFERENT account that never
        // connected it, must resolve to NOTHING — this is exactly the
        // over-count `seating_disclosure`'s pre-M3-fix version would have
        // produced by using the cross-account resolver instead.
        assert_eq!(
            resolve_google_identity_in_account(&pool, account_b, "shared-sub").await.unwrap(),
            None,
            "a sub connected under account A must not resolve under unrelated account B"
        );
        // Empty sub never resolves.
        assert_eq!(resolve_google_identity_in_account(&pool, account_a, "").await.unwrap(), None);

        // Determinism (ORDER BY updated_at DESC LIMIT 1): two different
        // CoralSwarm users under the SAME account connect the SAME sub —
        // an edge case, but a real one. The most recently updated row wins,
        // not whichever Postgres happens to return first.
        let owner_older = seed_user(&pool).await;
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, owner_user_id, provider_user_id, provider_workspace_id, access_token, refresh_token, updated_at) \
             VALUES ($1, 'google_chat', 'test-scope', $2, 'dup-sub', 'workspace-older', 'enc', 'enc', now() - interval '1 hour')",
        )
        .bind(account_a)
        .bind(owner_older)
        .execute(&pool)
        .await
        .unwrap();
        let owner_newer = seed_user(&pool).await;
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, owner_user_id, provider_user_id, provider_workspace_id, access_token, refresh_token, updated_at) \
             VALUES ($1, 'google_chat', 'test-scope', $2, 'dup-sub', 'workspace-newer', 'enc', 'enc', now())",
        )
        .bind(account_a)
        .bind(owner_newer)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            resolve_google_identity_in_account(&pool, account_a, "dup-sub").await.unwrap(),
            Some(owner_newer),
            "the most recently updated row must win deterministically, not an arbitrary one"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn resolve_chat_space_accounts_returns_every_account_holding_a_container_row_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping resolve_chat_space_accounts_returns_every_account_holding_a_container_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_space_accounts";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Empty / unknown space: no accounts.
        assert_eq!(resolve_chat_space_accounts(&pool, "").await.unwrap(), Vec::<Uuid>::new());
        assert_eq!(resolve_chat_space_accounts(&pool, "spaces/unknown").await.unwrap(), Vec::<Uuid>::new());

        // H1's real scenario: TWO different CoralSwarm accounts each
        // independently added the app to the SAME physical space.
        let account_a = seed_account(&pool, seed_user(&pool).await).await;
        let account_b = seed_account(&pool, seed_user(&pool).await).await;
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_a, None, &test_space("spaces/shared")).await.unwrap();
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_b, None, &test_space("spaces/shared")).await.unwrap();
        // A third account with an UNRELATED space must not show up.
        let account_c = seed_account(&pool, seed_user(&pool).await).await;
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_c, None, &test_space("spaces/other")).await.unwrap();

        let mut accounts = resolve_chat_space_accounts(&pool, "spaces/shared").await.unwrap();
        accounts.sort();
        let mut expected = vec![account_a, account_b];
        expected.sort();
        assert_eq!(accounts, expected, "must return EVERY account holding a row for this space, not just one");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn removed_from_space_fan_out_deactivates_every_account_never_the_removers_own_pg() {
        // The H1 proof at the connectors layer: exactly the sequence
        // `routes::google_push::handle_chat_removed_from_space` runs —
        // resolve accounts BY SPACE, then deactivate each — with NO
        // dependency on any identity for the removing member at all
        // (unlike the pre-fix version, which used the remover's own `sub`
        // and therefore never fired for the overwhelmingly common case: a
        // remover who has never connected CoralSwarm).
        let Some(base) = base_url() else {
            eprintln!("skipping removed_from_space_fan_out_deactivates_every_account_never_the_removers_own_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_removal_fan_out";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_a = seed_account(&pool, seed_user(&pool).await).await;
        let account_b = seed_account(&pool, seed_user(&pool).await).await;
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_a, None, &test_space("spaces/shared")).await.unwrap();
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_b, None, &test_space("spaces/shared")).await.unwrap();

        // Resolve by SPACE ONLY — no remover identity involved anywhere in
        // this test, proving the fix doesn't secretly still need one.
        let accounts = resolve_chat_space_accounts(&pool, "spaces/shared").await.unwrap();
        assert_eq!(accounts.len(), 2);
        for account_id in accounts {
            let deactivated = record_removed_from_space(&pool, account_id, "spaces/shared").await.unwrap();
            assert!(deactivated);
        }

        let rows_a = list_watched_spaces(&pool, account_a).await.unwrap();
        let rows_b = list_watched_spaces(&pool, account_b).await.unwrap();
        assert!(!rows_a["spaces/shared"].active, "account A's row must be deactivated");
        assert!(!rows_b["spaces/shared"].active, "account B's row must ALSO be deactivated — H1's whole point");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn pick_space_destination_refuses_an_inactive_removed_space_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping pick_space_destination_refuses_an_inactive_removed_space_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_pick_inactive";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();

        let account_id = Uuid::new_v4();
        let graph_id = seed_tenant_graph(&pool, account_id).await;
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_id, None, &test_space("spaces/removed")).await.unwrap();

        // The app is removed BEFORE anyone picks a destination.
        let deactivated = record_removed_from_space(&pool, account_id, "spaces/removed").await.unwrap();
        assert!(deactivated);

        // A pick attempt now must be refused (L4) — the row still matches
        // `mode IS NULL`, but D6's gate (the app currently being a member
        // of the space) no longer holds.
        let result = pick_space_destination(
            &pool,
            &pool,
            AccountPlane::Personal,
            account_id,
            "spaces/removed",
            PickedDestination::Single(graph_id),
        )
        .await;
        assert!(
            matches!(result, Err(PickError::NotConnected)),
            "picking a destination for an inactive (removed) space must be refused — got {result:?}"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    async fn seed_tenant_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'A reef', 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn pick_space_destination_refuses_an_unowned_graph_and_a_never_watched_space_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping pick_space_destination_refuses_an_unowned_graph_and_a_never_watched_space_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_pick_refusals";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();

        let account_a = Uuid::new_v4();
        let account_b = Uuid::new_v4();
        let graph_a = seed_tenant_graph(&pool, account_a).await;
        let graph_b = seed_tenant_graph(&pool, account_b).await;

        // Never-watched space, but an OWNED graph (isolates "no row exists"
        // from the ownership check, which runs first and would otherwise
        // report GraphNotOwned instead): NotConnected, not a silent success.
        let never_watched = pick_space_destination(
            &pool,
            &pool,
            AccountPlane::Personal,
            account_a,
            "spaces/never-added",
            PickedDestination::Single(graph_a),
        )
        .await;
        assert!(matches!(never_watched, Err(PickError::NotConnected)));

        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_a, None, &test_space("spaces/BBB"))
            .await
            .unwrap();

        // Cross-account graph: refused, never persisted.
        let cross = pick_space_destination(
            &pool,
            &pool,
            AccountPlane::Personal,
            account_a,
            "spaces/BBB",
            PickedDestination::Single(graph_b),
        )
        .await;
        assert!(matches!(cross, Err(PickError::GraphNotOwned)));

        // FanOut on the Personal plane: refused.
        let fanout_on_personal = pick_space_destination(
            &pool,
            &pool,
            AccountPlane::Personal,
            account_a,
            "spaces/BBB",
            PickedDestination::FanOut,
        )
        .await;
        assert!(matches!(fanout_on_personal, Err(PickError::FanoutRequiresOrgPlane)));

        // Still unrouted after every refusal above.
        let rows = list_watched_spaces(&pool, account_a).await.unwrap();
        assert_eq!(rows["spaces/BBB"].mode, None);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The `#705`-class proof (design doc §3.8): `record_added_to_space` +
    /// `resolve_google_identity_by_sub` genuinely split their reads/writes
    /// across TWO SEPARATE physical databases — the watched-space row lands
    /// on CONTENT (tenant), the identity resolution reads CONTROL only
    /// (`oauth_tokens` does not exist on the tenant tree at all, so a
    /// caller that accidentally handed the tenant pool to
    /// `resolve_google_identity_by_sub` would fail outright, not silently
    /// return the wrong thing).
    #[tokio::test]
    async fn added_to_space_and_identity_resolution_genuinely_split_across_two_pools_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping added_to_space_and_identity_resolution_genuinely_split_across_two_pools_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_g43_two_pool_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_g43_two_pool_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        let control_conn = recreate(&base, control_db).await;
        control_conn.close().await.ok();
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        let tenant_conn = recreate(&base, tenant_db).await;
        tenant_conn.close().await.ok();

        let control_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();
        let tenant_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();

        let owner = seed_user(&control_pool).await;
        let account_id = seed_account(&control_pool, owner).await;
        seed_oauth_token(&control_pool, account_id, owner, "google_chat", "sub-carol").await;
        let graph_id = seed_tenant_graph(&tenant_pool, account_id).await;

        // Identity resolution reads CONTROL only.
        assert_eq!(
            resolve_google_identity_by_sub(&control_pool, "sub-carol").await.unwrap(),
            Some((account_id, owner))
        );

        // The watched-space row is written to TENANT (content), never
        // control — `record_added_to_space(control_pool, tenant_pool, ...)`
        // called with the two GENUINELY different pools above.
        record_added_to_space(
            &control_pool,
            &tenant_pool,
            AccountPlane::Personal,
            account_id,
            Some(owner),
            &test_space("spaces/CCC"),
        )
        .await
        .unwrap();
        let on_tenant: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/CCC'",
        )
        .bind(account_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
        assert_eq!(on_tenant, 1, "the watched-space row must land on the TENANT (content) pool");

        // `oauth_tokens` DOES have a schema-parity twin on the tenant tree
        // (migrations_tenant/0001_tenant_baseline.sql, widened by 0059/0060
        // — it is on `tenant_rls_pg.rs`'s `DELIBERATELY_UNCOVERED` roster,
        // inert rather than absent), so a swapped-pool bug here would NOT
        // hard-fail the way it does for Gmail's `resolve_watch_owners_for_push`
        // (which calls a CONTROL-only SECURITY DEFINER function with no
        // tenant-tree definition at all). The real proof is a genuine-two-
        // database one instead: the row seeded only on `control_pool` is
        // simply NOT THERE on `tenant_pool`'s (schema-identical but
        // data-empty) copy of the same table — proving these are two
        // actually different physical databases, not two handles onto one.
        let oauth_tokens_on_tenant: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens")
            .fetch_one(&tenant_pool)
            .await
            .unwrap();
        assert_eq!(
            oauth_tokens_on_tenant, 0,
            "the control-seeded oauth_tokens row must not be visible on the tenant pool — these \
             must be genuinely separate physical databases"
        );

        // Pick a destination via the real graph on the tenant pool — proves
        // `pick_space_destination`'s ownership check also runs on TENANT
        // (a `graphs` row seeded only on `control_pool` would not satisfy
        // it there).
        let picked = pick_space_destination(
            &control_pool,
            &tenant_pool,
            AccountPlane::Personal,
            account_id,
            "spaces/CCC",
            PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        assert!(picked.newly_routed);

        control_pool.close().await;
        tenant_pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {control_db} WITH (FORCE)").as_str()).await.ok();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {tenant_db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── CS-432 (D17 point 4) — `record_added_to_space`'s plane and the
    // control-plane-mismatch warning ──────────────────────────────────────

    /// Pure — no I/O. Mirrors `connectors::slack::UNMATCHED_NOTICE_COOLDOWN`'s
    /// own untested-in-isolation convention broken here on purpose: this
    /// cooldown is CS-432's only signal that the mismatch warning fired at
    /// all (no tracing subscriber wired into this crate's test harness), so
    /// `routes::google_push`'s own pg test asserts against it directly —
    /// pinning the gate's basic shape here first.
    #[test]
    fn should_warn_chat_plane_mismatch_cooldown_is_per_account_and_space() {
        let account_a = Uuid::new_v4();
        let account_b = Uuid::new_v4();

        assert!(
            should_warn_chat_plane_mismatch(account_a, "spaces/ONE"),
            "the first check for a fresh (account, space) must be allowed to warn"
        );
        assert!(
            !should_warn_chat_plane_mismatch(account_a, "spaces/ONE"),
            "a second check for the SAME (account, space) within the cooldown must not"
        );
        assert!(
            should_warn_chat_plane_mismatch(account_a, "spaces/TWO"),
            "a DIFFERENT space for the same account is a different key"
        );
        assert!(
            should_warn_chat_plane_mismatch(account_b, "spaces/ONE"),
            "a DIFFERENT account for the same space is a different key"
        );
    }

    #[tokio::test]
    async fn chat_space_row_exists_returns_true_only_for_the_exact_account_and_space_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping chat_space_row_exists_returns_true_only_for_the_exact_account_and_space_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_cs432_row_exists";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner_a = seed_user(&pool).await;
        let owner_b = seed_user(&pool).await;
        let account_a = seed_account(&pool, owner_a).await;
        // Distinct owner — `accounts.owner_user_id` has a one-personal-
        // account-per-owner unique constraint (`uq_accounts_one_personal_
        // per_owner`), so a second account for the SAME owner would fail
        // this seed, not the assertions below.
        let account_b = seed_account(&pool, owner_b).await;
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_a, None, &test_space("spaces/EXISTS"))
            .await
            .unwrap();

        assert!(chat_space_row_exists(&pool, account_a, "spaces/EXISTS").await.unwrap());
        assert!(
            !chat_space_row_exists(&pool, account_a, "spaces/NOPE").await.unwrap(),
            "a different space_id must not match"
        );
        assert!(
            !chat_space_row_exists(&pool, account_b, "spaces/EXISTS").await.unwrap(),
            "a different account_id must not match, even for the same space_id"
        );

        pool.close().await;
    }

    /// **The CS-432 write-plane regression test.** `record_added_to_space`
    /// called with `AccountPlane::Org` must land the row on `tenant_pool`,
    /// never `control_pool` — and must flag (once) a row LEFT BEHIND on
    /// control for the exact same `(account_id, space_id)`, the pre-CS-432
    /// shape every write used to take regardless of tenancy. A DIFFERENT
    /// space with no such leftover must not be flagged at all.
    #[tokio::test]
    async fn record_added_to_space_org_plane_writes_tenant_and_flags_a_stale_control_row_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping record_added_to_space_org_plane_writes_tenant_and_flags_a_stale_control_row_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_cs432_write_plane_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_cs432_write_plane_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        let control_conn = recreate(&base, control_db).await;
        control_conn.close().await.ok();
        let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        let tenant_conn = recreate(&base, tenant_db).await;
        tenant_conn.close().await.ok();

        let control_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();
        let tenant_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();

        let owner = seed_user(&control_pool).await;
        let account_id = seed_account(&control_pool, owner).await;

        // A row LEFT BEHIND on control from before CS-432 — the exact D17
        // point 4 shape (every write, org or not, used to land here).
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, active) \
             VALUES ($1, 'spaces/LEGACY', 'SPACE', true)",
        )
        .bind(account_id)
        .execute(&control_pool)
        .await
        .unwrap();

        // The fixed write: `AccountPlane::Org`, genuinely different pools.
        record_added_to_space(
            &control_pool,
            &tenant_pool,
            AccountPlane::Org,
            account_id,
            Some(owner),
            &test_space("spaces/LEGACY"),
        )
        .await
        .unwrap();

        let on_tenant: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/LEGACY'",
        )
        .bind(account_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
        assert_eq!(on_tenant, 1, "an Org-plane write must land on the TENANT pool");

        let on_control: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/LEGACY'",
        )
        .bind(account_id)
        .fetch_one(&control_pool)
        .await
        .unwrap();
        assert_eq!(
            on_control, 1,
            "the pre-existing control row must be left exactly as it was — CS-432 does not \
             migrate it"
        );

        // The mismatch check must have fired (and consumed its cooldown
        // slot) for this exact (account, space) — a legacy control row
        // genuinely exists.
        assert!(
            !should_warn_chat_plane_mismatch(account_id, "spaces/LEGACY"),
            "the plane-mismatch warning must have fired for a space with a real control-plane \
             leftover"
        );

        // A FRESH space with no control-plane leftover must write only to
        // tenant. The mismatch check still RUNS (the cooldown gates the
        // check itself, not just a positive result — see
        // `chat_plane_mismatch_cooldown`'s own doc comment — so its slot is
        // consumed here too; `chat_space_row_exists_returns_true_only_for_
        // the_exact_account_and_space_pg` is the test that pins the check
        // correctly reporting `false` when nothing is actually there).
        record_added_to_space(
            &control_pool,
            &tenant_pool,
            AccountPlane::Org,
            account_id,
            Some(owner),
            &test_space("spaces/FRESH"),
        )
        .await
        .unwrap();
        let fresh_on_tenant: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/FRESH'",
        )
        .bind(account_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
        assert_eq!(fresh_on_tenant, 1);
        let fresh_on_control: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/FRESH'",
        )
        .bind(account_id)
        .fetch_one(&control_pool)
        .await
        .unwrap();
        assert_eq!(fresh_on_control, 0, "no leftover ever existed for this space — control must stay empty");
        assert!(
            !chat_space_row_exists(&control_pool, account_id, "spaces/FRESH").await.unwrap(),
            "the underlying detection query itself must report no control-plane row for a space \
             that never had one — the cooldown gate above only proves a check ran, not what it found"
        );

        control_pool.close().await;
        tenant_pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {control_db} WITH (FORCE)").as_str()).await.ok();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {tenant_db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Personal (and, by the same code path, personal-org) behaviour is
    /// UNCHANGED by CS-432: `AccountPlane::Personal` never runs the
    /// mismatch check (control and tenant are the SAME pool for this
    /// plane, so "left behind on the wrong plane" cannot occur), and the
    /// row lands on the single shared pool exactly as every pre-existing
    /// test in this module already assumes.
    #[tokio::test]
    async fn record_added_to_space_personal_plane_never_checks_for_a_mismatch_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping record_added_to_space_personal_plane_never_checks_for_a_mismatch_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_cs432_personal_unchanged";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        record_added_to_space(&pool, &pool, AccountPlane::Personal, account_id, Some(owner), &test_space("spaces/PERSONAL"))
            .await
            .unwrap();

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/PERSONAL'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1, "the row must still land on the single shared pool, exactly as before CS-432");
        assert!(
            should_warn_chat_plane_mismatch(account_id, "spaces/PERSONAL"),
            "AccountPlane::Personal must never consume the mismatch cooldown — there is no \
             second plane to be mismatched against"
        );

        pool.close().await;
    }

    #[tokio::test]
    async fn seating_disclosure_degrades_to_unavailable_not_zero_when_the_roster_read_fails_pg() {
        // No live Chat API reachable from this test harness (same
        // constraint `connectors::slack`'s own doc comment gives for a
        // "no HTTP-mock harness" case — see `matches_and_persists_across_
        // two_genuinely_separate_pools_pg`'s header there): `list_members`
        // cannot reach a real `chat.googleapis.com` here, so this proves
        // the fail-closed half of the contract end-to-end (`available:
        // false`, never a false "zero members" reading) rather than the
        // counting arithmetic itself, which `unmapped_is_derived_never_
        // independent` above already pins as a pure function.
        let Some(base) = base_url() else {
            eprintln!("skipping seating_disclosure_degrades_to_unavailable_not_zero_when_the_roster_read_fails_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g43_seating_disclosure";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let http = reqwest::Client::new();
        let disclosure = seating_disclosure(&http, &pool, Uuid::new_v4(), "fake-token", "spaces/AAAAAAAAAAA").await;
        assert!(!disclosure.available, "no live Chat API reachable from this test — must report unavailable");
        assert_eq!(disclosure.total_humans, 0);
        assert_eq!(disclosure.matched, 0);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // =======================================================================
    // G4.4 — the ingest half
    // =======================================================================

    // ── `decide_after_page`, ×4 boundary (#677's CRITICAL) ─────────────────
    //
    // Ported with the function itself from `google_gmail::decide_after_page`
    // (`google_gmail.rs:618`-`:641`), which ported them from
    // `google_calendar`. The property under test is ONE thing said four
    // ways: the TERMINAL-page check must beat the budget check.

    #[test]
    fn decide_after_page_terminal_page_under_budget_finishes_and_is_terminal() {
        match decide_after_page(None, 1, 10) {
            PageDecision::Done {
                resume_page_token,
                terminal,
            } => {
                assert_eq!(resume_page_token, None);
                assert!(terminal, "a terminal page must report terminal");
            }
            _ => panic!("a terminal page must finish the pass"),
        }
    }

    #[test]
    fn decide_after_page_terminal_page_at_budget_still_finishes_as_terminal() {
        // THE boundary. Checking the budget first would report a terminal
        // page as `resume_page_token: None, terminal: false`, which a
        // resume-from-`None` caller reads as "start over" — an infinite
        // re-walk of any listing that happens to end exactly at the budget.
        match decide_after_page(None, 10, 10) {
            PageDecision::Done {
                resume_page_token,
                terminal,
            } => {
                assert_eq!(resume_page_token, None);
                assert!(terminal, "TERMINAL BEATS BUDGET — this is #677's CRITICAL finding");
            }
            _ => panic!("a terminal page must finish the pass even at budget"),
        }
    }

    #[test]
    fn decide_after_page_non_terminal_under_budget_continues() {
        match decide_after_page(Some("tok".into()), 1, 10) {
            PageDecision::Continue(next) => assert_eq!(next, "tok"),
            _ => panic!("a non-terminal page under budget must keep paging"),
        }
    }

    #[test]
    fn decide_after_page_non_terminal_at_budget_stops_with_a_resume_cursor() {
        match decide_after_page(Some("tok".into()), 10, 10) {
            PageDecision::Done {
                resume_page_token,
                terminal,
            } => {
                assert_eq!(resume_page_token.as_deref(), Some("tok"));
                assert!(!terminal, "an interrupted pass must never claim it reached the end");
            }
            _ => panic!("a non-terminal page at budget must stop and hand back a resume cursor"),
        }
    }

    #[test]
    fn decide_after_page_treats_an_empty_next_page_token_as_terminal() {
        // Google returns `"nextPageToken": ""` on the terminal page of some
        // listings (`space_events_list.json`'s own fixture does exactly
        // that). Treating an empty string as a real cursor would send
        // `&pageToken=` upstream forever.
        match decide_after_page(Some(String::new()), 1, 10) {
            PageDecision::Done { terminal, .. } => assert!(terminal),
            _ => panic!("an empty nextPageToken is terminal, not a cursor"),
        }
    }

    // ── Subscription request shape (§4.1 / D5, risk 2) ─────────────────────

    #[test]
    fn subscription_body_omits_payload_options() {
        // Risk 2 asks for this to be pinned by a TEST, not a comment:
        // including `payloadOptions` drops the maximum TTL from 7 days to 4
        // hours, which would make `RENEW_WINDOW_HOURS = 24` wrong by ~6x and
        // every deploy window a data-loss window.
        let body = subscription_request_body("projects/p/topics/chat-events");
        assert!(
            body.get("payloadOptions").is_none(),
            "payloadOptions must NEVER be sent (D5, §4.2) — including it silently caps the \
             subscription TTL at 4 hours"
        );
        assert_eq!(body["ttl"], "0s", "ttl \"0s\" asks for the API maximum");
        assert_eq!(body["targetResource"], CHAT_ALL_SPACES_TARGET);
        assert_eq!(body["notificationEndpoint"]["pubsubTopic"], "projects/p/topics/chat-events");
    }

    #[test]
    fn subscribed_event_types_exclude_updates_and_reactions() {
        // §5.3's "edits reported upstream, deliberately not honoured in v1"
        // declaration is only true as long as we do not subscribe to
        // `message.v1.updated`. Conformance test 8′'s pure half.
        assert!(
            !CHAT_SUBSCRIBED_EVENT_TYPES
                .iter()
                .any(|t| t.contains("message.v1.updated")),
            "subscribing to message.v1.updated would silently turn §5.3's declared limitation \
             into an implied guarantee we do not implement"
        );
        for absent in ["reaction.v1", "space.v1", "readState"] {
            assert!(
                !CHAT_SUBSCRIBED_EVENT_TYPES.iter().any(|t| t.contains(absent)),
                "{absent} events are deliberately not subscribed — a narrower event set is a \
                 smaller consent story and nothing downstream consumes them"
            );
        }
        assert_eq!(CHAT_SUBSCRIBED_EVENT_TYPES.len(), 4);
    }

    #[test]
    fn subscription_from_operation_parses_the_create_fixture() {
        const FIXTURE: &str =
            include_str!("../../tests/fixtures/google_chat/subscriptions_create_response.json");
        let op: crate::workers::google_watch_renewal::Operation = serde_json::from_str(FIXTURE).unwrap();
        assert!(op.done);
        let (name, expires_at) = subscription_from_operation(&op).unwrap();
        assert_eq!(name, "subscriptions/7f3a9c21");
        assert_eq!(expires_at.to_rfc3339(), "2026-08-08T00:00:00+00:00");
        // The 7-day TTL D5 buys — a create response reporting ~4 hours would
        // mean `payloadOptions` leaked into the request.
        let created = DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z").unwrap();
        assert!(
            (expires_at - created.with_timezone(&Utc)).num_days() >= 6,
            "a subscription created without payloadOptions must come back with a ~7-day TTL"
        );
    }

    #[test]
    fn subscription_from_operation_refuses_an_errored_or_empty_operation() {
        let errored: crate::workers::google_watch_renewal::Operation =
            serde_json::from_value(serde_json::json!({
                "name": "operations/x", "done": true, "error": {"code": 7, "message": "denied"}
            }))
            .unwrap();
        assert!(subscription_from_operation(&errored).is_err());

        let no_name: crate::workers::google_watch_renewal::Operation =
            serde_json::from_value(serde_json::json!({
                "name": "operations/x", "done": true, "response": {"expireTime": "2026-08-08T00:00:00Z"}
            }))
            .unwrap();
        assert!(
            subscription_from_operation(&no_name).is_err(),
            "no subscription name means no channel_id to renew or revoke later — refuse rather \
             than writing a row with an empty key"
        );
    }

    // ── Catch-up filter and watermark (§2.7) ───────────────────────────────

    #[test]
    fn space_events_filter_names_both_message_types_and_never_a_batch_type() {
        let f = space_events_filter(Some("2026-08-01T00:00:00+00:00"));
        assert!(f.contains("eventTypes:\"google.workspace.chat.message.v1.created\""));
        assert!(f.contains("eventTypes:\"google.workspace.chat.message.v1.deleted\""));
        assert!(f.contains(" OR "), "several event types are OR-ed (§2.7)");
        assert!(f.contains(" AND startTime="), "the time bound is AND-ed, never OR-ed (§2.7)");
        assert!(
            !f.contains("batch"),
            "§2.7: \"Omit batch event types in your filter. The request automatically returns \
             any related batch events.\""
        );
        // No time bound at all is legal; the filter is still valid.
        assert!(!space_events_filter(None).contains("startTime"));
    }

    #[test]
    fn floored_watermark_floors_at_the_28_day_retention_bound() {
        let now = DateTime::parse_from_rfc3339("2026-08-30T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        // Well inside the window: preserved exactly.
        let recent = "2026-08-29T00:00:00+00:00";
        assert_eq!(floored_watermark(Some(recent), now), recent);
        // Older than 28 days: floored, never sent upstream as-is (Google
        // rejects it, and the events before it are unrecoverable anyway).
        let ancient = floored_watermark(Some("2020-01-01T00:00:00+00:00"), now);
        assert_eq!(ancient, (now - chrono::Duration::days(28)).to_rfc3339());
        // Unset (first catch-up after a backfill) and unparseable both floor
        // to the same bound rather than erroring or defaulting to "now".
        assert_eq!(floored_watermark(None, now), ancient);
        assert_eq!(floored_watermark(Some("not-a-timestamp"), now), ancient);
    }

    // ── `spaceEvents.list` decode — the UNVERIFIED payload, handled ────────

    #[test]
    fn space_events_page_maps_created_deleted_and_batch_events_to_message_names() {
        const FIXTURE: &str = include_str!("../../tests/fixtures/google_chat/space_events_list.json");
        let page: SpaceEventsListResponse = serde_json::from_str(FIXTURE).unwrap();
        let (created, deleted) = process_space_events_page(page);
        assert_eq!(
            created,
            vec![
                "spaces/AAAAAAAAAAA/messages/msg-0002",
                "spaces/AAAAAAAAAAA/messages/msg-0006",
                "spaces/AAAAAAAAAAA/messages/msg-0007",
            ],
            "a batchCreated event's messages count exactly like individual creates — Google \
             delivers batch variants automatically for any subscribed type (§2.7)"
        );
        assert_eq!(
            deleted,
            vec!["spaces/AAAAAAAAAAA/messages/msg-0000-typo"],
            "a deletion event hands back the message resource name, which IS the source_id \
             verbatim (§3.6)"
        );
    }

    #[test]
    fn space_events_page_tolerates_an_unknown_shape_without_panicking_or_misclassifying() {
        // The `SpaceEvent` per-event-type body is UNVERIFIED (§2.7, risk 14;
        // the fixture's own README says so). A shape surprise must be a
        // contained, counted skip — never a panic, never a 5xx, and above
        // all never a CREATED message routed into the DELETION path.
        let page: SpaceEventsListResponse = serde_json::from_value(serde_json::json!({
            "spaceEvents": [
                { "eventType": "google.workspace.chat.message.v1.created", "somethingElse": {"message": {"name": "x"}} },
                { "eventType": "google.workspace.chat.membership.v1.created" },
                { "eventType": "google.workspace.chat.message.v1.deleted",
                  "messageDeletedEventData": { "message": { "name": "   " } } },
                { "eventType": "google.workspace.chat.message.v1.created",
                  "messageCreatedEventData": { "message": { "name": "spaces/S/messages/ok" } } }
            ],
            "nextPageToken": ""
        }))
        .unwrap();
        let (created, deleted) = process_space_events_page(page);
        assert_eq!(created, vec!["spaces/S/messages/ok"]);
        assert!(
            deleted.is_empty(),
            "an empty/whitespace message name is skipped, never propagated as a deletion of \"\""
        );
    }

    // ── Backfill page splitting (§4.4) ─────────────────────────────────────

    #[test]
    fn backfill_fixture_pages_split_into_a_non_terminal_page_and_a_terminal_one() {
        const P1: &str = include_str!("../../tests/fixtures/google_chat/messages_list_page1.json");
        const P2: &str = include_str!("../../tests/fixtures/google_chat/messages_list_page2.json");
        let page1: MessagesListResponse = serde_json::from_str(P1).unwrap();
        let page2: MessagesListResponse = serde_json::from_str(P2).unwrap();

        assert!(!page1.messages.is_empty() && !page2.messages.is_empty());
        assert!(page1.next_page_token.is_some(), "page 1 is non-terminal");
        assert!(page2.next_page_token.is_none(), "page 2 is the terminal page");

        // The pass-level decision each page drives, with a budget large
        // enough that only the terminal flag can decide.
        assert!(matches!(
            decide_after_page(page1.next_page_token.clone(), page1.messages.len(), SYNC_PASS_BUDGET),
            PageDecision::Continue(_)
        ));
        assert!(matches!(
            decide_after_page(page2.next_page_token.clone(), page2.messages.len(), SYNC_PASS_BUDGET),
            PageDecision::Done { terminal: true, .. }
        ));

        // And with a budget of ONE, page 1 stops WITH its cursor — the
        // resumable half of "no page cap, budgeted per pass".
        match decide_after_page(page1.next_page_token.clone(), page1.messages.len(), 1) {
            PageDecision::Done {
                resume_page_token,
                terminal,
            } => {
                assert_eq!(resume_page_token, page1.next_page_token);
                assert!(!terminal);
            }
            _ => panic!("an over-budget non-terminal page must stop with a resume cursor"),
        }
    }

    // ── Roster counting over the 7-member fixture (§3.4) ───────────────────

    fn fixture_roster() -> SpaceRoster {
        const FIXTURE: &str = include_str!("../../tests/fixtures/google_chat/members_list.json");
        let parsed: MembersListResponse = serde_json::from_str(FIXTURE).unwrap();
        roster_from_memberships(&parsed.memberships)
    }

    #[test]
    fn roster_counting_applies_slacks_rules_verbatim_over_the_seven_member_fixture() {
        let roster = fixture_roster();
        assert_eq!(
            roster.total_humans(),
            4,
            "M = JOINED, non-bot, non-group humans: users/1001..1004"
        );
        assert_eq!(roster.bots_excluded, 1, "`member.type == BOT` is excluded outright (§3.4)");
        assert_eq!(
            roster.not_joined_excluded, 1,
            "an INVITED member has not accepted and is not yet part of the audience (§2.4)"
        );
        assert_eq!(
            roster.group_memberships, 1,
            "a Google Group is counted SEPARATELY (§3.4a) — \"we cannot enumerate these at all\" \
             is a different statement from \"these people have no CoralSwarm account\""
        );
        let subs: Vec<&str> = roster.members.iter().map(|m| m.sub.as_str()).collect();
        assert_eq!(subs, vec!["1001", "1002", "1003", "1004"]);
        assert!(
            roster.members.iter().all(|m| !m.sub.starts_with("users/")),
            "the `users/` prefix is stripped so the sub joins directly against \
             oauth_tokens.provider_user_id (§3.3)"
        );
    }

    #[test]
    fn a_group_bearing_space_is_still_eligible_for_a_shared_destination() {
        // Founder answer Q3 = ALLOW + DISCLOSE, not refuse (§3.4a). Contract
        // §4's C3 row is about being unable to enumerate the roster AT ALL,
        // not part of it — so the presence of a group must not make the
        // roster unusable, only incompletely disclosable.
        let roster = fixture_roster();
        assert!(roster.group_memberships > 0);
        assert!(
            roster.total_humans() > 0,
            "a group-bearing space still yields a usable human roster — refusing it outright \
             would be the stricter reading founder answer Q3 declined"
        );
    }

    // ── Domain directory response parsing (pure — no network, no DB) ───────

    #[test]
    fn directory_map_from_people_strips_the_people_prefix_and_keys_like_a_chat_sub() {
        // `Person.resourceName` ("people/{id}") and Chat's `User.name`
        // ("users/{id}") share the SAME bare numeric id — this is the
        // equivalence the whole "Domain directory" section depends on, so
        // it is pinned here at the parsing boundary, directly analogous to
        // `bare_sub_strips_the_users_prefix` for the Chat side.
        const JSON: &str = r#"{
            "people": [
                {
                    "resourceName": "people/1004",
                    "emailAddresses": [{"value": "priya.raman@k12isd.example", "metadata": {"verified": true, "source": {"type": "DOMAIN_PROFILE"}}}],
                    "names": [{"displayName": "Priya Raman"}]
                }
            ]
        }"#;
        let parsed: ListDirectoryPeopleResponse = serde_json::from_str(JSON).unwrap();
        let map = directory_map_from_people(parsed.people);
        let entry = map.get("1004").expect("the `people/` prefix must be stripped so this keys like a Chat sub");
        assert_eq!(entry.email, "priya.raman@k12isd.example");
        assert_eq!(entry.display_name, "Priya Raman");
    }

    #[test]
    fn directory_map_from_people_drops_a_person_with_no_usable_email() {
        // A directory profile may have field-level visibility restrictions
        // (Google's own note) — a person with every `emailAddresses` entry
        // blank, or the list empty, must be DROPPED, never inserted with an
        // empty-string key that could later false-positive-match an empty
        // `normalize_email` result.
        const JSON: &str = r#"{
            "people": [
                {"resourceName": "people/1005", "emailAddresses": [], "names": [{"displayName": "No Email"}]},
                {"resourceName": "people/1006", "emailAddresses": [{"value": "  ", "metadata": {"verified": true, "source": {"type": "DOMAIN_PROFILE"}}}], "names": []}
            ]
        }"#;
        let parsed: ListDirectoryPeopleResponse = serde_json::from_str(JSON).unwrap();
        let map = directory_map_from_people(parsed.people);
        assert!(map.is_empty(), "neither entry has a usable email — both must be dropped, not inserted blank");
    }

    /// CodeRabbit round 1 (#1005, Security & Privacy): FAILS WITHOUT THE
    /// FIX. An email with no `metadata` at all, `verified: false`, or a
    /// `source.type` other than `DOMAIN_PROFILE` must be treated exactly
    /// like a blank one — dropped, never persisted as a `verified_email`
    /// link. Three independent people, three independent reasons, one
    /// outcome: all three are unusable.
    #[test]
    fn directory_map_from_people_drops_an_unverified_or_non_domain_profile_email() {
        const JSON: &str = r#"{
            "people": [
                {"resourceName": "people/2001", "emailAddresses": [{"value": "no-metadata@k12isd.example"}], "names": []},
                {"resourceName": "people/2002", "emailAddresses": [{"value": "unverified@k12isd.example", "metadata": {"verified": false, "source": {"type": "DOMAIN_PROFILE"}}}], "names": []},
                {"resourceName": "people/2003", "emailAddresses": [{"value": "wrong-source@gmail.example", "metadata": {"verified": true, "source": {"type": "CONTACT"}}}], "names": []}
            ]
        }"#;
        let parsed: ListDirectoryPeopleResponse = serde_json::from_str(JSON).unwrap();
        let map = directory_map_from_people(parsed.people);
        assert!(
            map.is_empty(),
            "no-metadata, unverified, and non-domain-profile emails must all be dropped: {map:?}"
        );
    }

    /// A person with TWO email addresses — one unverified (listed first,
    /// which an earlier "first nonblank" rule would have picked), one
    /// verified — must resolve to the VERIFIED one, never silently prefer
    /// list order over verification.
    #[test]
    fn directory_map_from_people_skips_an_unverified_address_to_reach_a_verified_one() {
        const JSON: &str = r#"{
            "people": [
                {
                    "resourceName": "people/2004",
                    "emailAddresses": [
                        {"value": "unverified-first@k12isd.example", "metadata": {"verified": false, "source": {"type": "DOMAIN_PROFILE"}}},
                        {"value": "verified-second@k12isd.example", "metadata": {"verified": true, "source": {"type": "DOMAIN_PROFILE"}}}
                    ],
                    "names": []
                }
            ]
        }"#;
        let parsed: ListDirectoryPeopleResponse = serde_json::from_str(JSON).unwrap();
        let map = directory_map_from_people(parsed.people);
        assert_eq!(
            map.get("2004").expect("the verified address must still be found").email,
            "verified-second@k12isd.example"
        );
    }

    #[test]
    fn directory_map_from_people_falls_back_to_the_email_as_a_display_name_hint() {
        // Mirrors `extract_verified_slack_profile`'s own name-hint fallback
        // order: prefer a real display name, fall back to the email itself
        // rather than leaving a fresh `connector_identities` row unnamed.
        const JSON: &str = r#"{
            "people": [
                {"resourceName": "people/1007", "emailAddresses": [{"value": "noname@k12isd.example", "metadata": {"verified": true, "source": {"type": "DOMAIN_PROFILE"}}}], "names": []}
            ]
        }"#;
        let parsed: ListDirectoryPeopleResponse = serde_json::from_str(JSON).unwrap();
        let map = directory_map_from_people(parsed.people);
        assert_eq!(map.get("1007").unwrap().display_name, "noname@k12isd.example");
    }

    /// CodeRabbit round 1 (#1005, Thread 23): `directory_for_reader`'s own
    /// caching contract, proven with a call-counting closure — no network,
    /// no database, no sweep infrastructure required.
    mod directory_for_reader_tests {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[tokio::test]
        async fn a_second_call_for_the_same_reader_never_calls_fetch_again() {
            let mut cache = DomainDirectoryCache::new();
            let reader = Uuid::new_v4();
            let calls = AtomicUsize::new(0);
            let mut entry = HashMap::new();
            entry.insert("u1".to_string(), DirectoryEntry { email: "u1@example.com".into(), display_name: "U1".into() });

            let (first, first_fresh) = directory_for_reader(&mut cache, reader, || {
                calls.fetch_add(1, Ordering::SeqCst);
                let entry = entry.clone();
                async move { Ok(entry) }
            })
            .await;
            assert!(first.is_ok());
            assert!(first_fresh, "the first call for a never-seen reader must be a genuine fetch");

            let (second, second_fresh) = directory_for_reader(&mut cache, reader, || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { panic!("must not be called — the cache should have served this one") }
            })
            .await;
            assert!(second.is_ok());
            assert!(!second_fresh, "a cache hit is not a fresh fetch");
            assert_eq!(second.unwrap(), first.unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), 1, "fetch must run at most once per reader");
        }

        /// The exact wording of the finding: *"Cache the result, including
        /// an unavailable result, so each reader is fetched at most once per
        /// pass."* An `Err` is cached too, not only an `Ok`.
        #[tokio::test]
        async fn an_unavailable_result_is_cached_too_not_only_a_success() {
            let mut cache = DomainDirectoryCache::new();
            let reader = Uuid::new_v4();
            let calls = AtomicUsize::new(0);

            let (first, first_fresh) = directory_for_reader(&mut cache, reader, || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Err("rate limited".to_string()) }
            })
            .await;
            assert_eq!(first, Err("rate limited".to_string()));
            assert!(first_fresh);

            let (second, second_fresh) = directory_for_reader(&mut cache, reader, || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { panic!("must not be called — the Err itself must be cached") }
            })
            .await;
            assert_eq!(second, Err("rate limited".to_string()));
            assert!(!second_fresh);
            assert_eq!(calls.load(Ordering::SeqCst), 1, "a cached failure must not re-fetch");
        }

        /// Two different readers never share a slot — a second account's
        /// elected reader in the same pass still gets its own fetch.
        #[tokio::test]
        async fn different_readers_each_get_their_own_fetch() {
            let mut cache = DomainDirectoryCache::new();
            let reader_a = Uuid::new_v4();
            let reader_b = Uuid::new_v4();
            let calls = AtomicUsize::new(0);

            let (_, fresh_a) =
                directory_for_reader(&mut cache, reader_a, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(HashMap::new()) }
                })
                .await;
            let (_, fresh_b) =
                directory_for_reader(&mut cache, reader_b, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(HashMap::new()) }
                })
                .await;
            assert!(fresh_a && fresh_b, "each reader's first lookup is a fresh fetch");
            assert_eq!(calls.load(Ordering::SeqCst), 2, "two distinct readers must fetch twice, not share a cache slot");
        }
    }

    #[test]
    fn unmapped_is_derived_from_the_matched_member_set_never_counted_independently() {
        // `slack.rs:561`-`:570`'s bug class, at the type level: the ONLY way
        // to build a `SeatingOutcome` correctly is `total - matched -
        // unavailable`.
        let roster = fixture_roster();
        let matched = 2usize; // users/1001 + users/1002, per the fixture README
        let outcome = SeatingOutcome {
            total_humans: roster.total_humans(),
            matched,
            seated: matched,
            unmapped: roster.total_humans().saturating_sub(matched),
            unavailable: 0,
            group_memberships: roster.group_memberships,
            matched_user_ids: Vec::new(),
        };
        assert_eq!(outcome.unmapped, 2, "4 JOINED humans, 2 mapped ⇒ 2 unmapped");
        assert_ne!(
            outcome.unmapped, outcome.group_memberships,
            "the group is NOT folded into `unmapped` — it is disclosed separately"
        );
    }

    #[test]
    fn a_wholesale_directory_failure_shows_up_as_unavailable_never_as_a_spike_in_unmapped() {
        // Coordinator review (chat-per-user-setup PR, 2026-09-20): the exact
        // property `unmapped`'s own doc comment names — a domain directory
        // that could not be read this pass must show up as `unavailable`,
        // not as "N members have no CoralSwarm account".
        let roster = fixture_roster();
        let matched = 0usize; // nobody sub-matched, and the directory itself failed
        let outcome = SeatingOutcome {
            total_humans: roster.total_humans(),
            matched,
            seated: matched,
            unmapped: roster.total_humans().saturating_sub(matched).saturating_sub(roster.total_humans()),
            unavailable: roster.total_humans(),
            group_memberships: roster.group_memberships,
            matched_user_ids: Vec::new(),
        };
        assert_eq!(outcome.unavailable, 4, "every JOINED human is UNCHECKED, not confirmed-unmatched");
        assert_eq!(
            outcome.unmapped, 0,
            "a wholesale directory failure must not inflate `unmapped` — that field means \"asked, \
             and genuinely no account\", which is not what happened here"
        );
    }

    #[test]
    fn roster_never_double_counts_a_repeated_member() {
        let memberships: Vec<ChatMembership> = serde_json::from_value(serde_json::json!([
            {"state": "JOINED", "member": {"name": "users/1", "type": "HUMAN", "displayName": "A"}},
            {"state": "JOINED", "member": {"name": "users/1", "type": "HUMAN", "displayName": "A"}},
            {"state": "JOINED", "member": {"name": "users/", "type": "HUMAN"}},
            {"state": "JOINED"}
        ]))
        .unwrap();
        let roster = roster_from_memberships(&memberships);
        assert_eq!(roster.total_humans(), 1);
        assert_eq!(
            roster.not_joined_excluded, 1,
            "a membership with neither `member` nor `groupMember` is accounted for, not silently \
             dropped from the arithmetic"
        );
    }

    /// Item 42. `roster_from_memberships` is capture. Guests stay in the
    /// roster so message ingest still attributes them. The invite/seat
    /// classifier is a separate function and is what skips them.
    #[test]
    fn chat_guest_affiliation_stays_in_the_capture_roster() {
        let memberships: Vec<ChatMembership> = serde_json::from_value(serde_json::json!([
            {"state": "JOINED", "affiliation": "EXTERNAL", "member": {"name": "users/9", "type": "HUMAN", "displayName": "Out"}},
            {"state": "JOINED", "affiliation": "MANAGED_EXTERNAL", "member": {"name": "users/8", "type": "HUMAN", "displayName": "Managed"}},
            {"state": "JOINED", "affiliation": "INTERNAL", "member": {"name": "users/1", "type": "HUMAN", "displayName": "In"}},
            {"state": "JOINED", "member": {"name": "users/2", "type": "HUMAN", "displayName": "No Field"}},
            {"state": "JOINED", "member": {"name": "users/bot", "type": "BOT", "displayName": "Bot"}}
        ]))
        .unwrap();
        let roster = roster_from_memberships(&memberships);
        assert_eq!(roster.total_humans(), 4, "externals are captured, not dropped");
        assert_eq!(roster.bots_excluded, 1);
        let subs: Vec<&str> = roster.members.iter().map(|m| m.sub.as_str()).collect();
        assert_eq!(subs, vec!["9", "8", "1", "2"]);
        let external = roster.members.iter().find(|m| m.sub == "9").unwrap();
        assert_eq!(external.resource_name, "users/9");
        assert_eq!(external.affiliation, "EXTERNAL");
    }

    fn one_anchor(domain: &str) -> Vec<String> {
        vec![domain.to_string()]
    }

    /// Item 42. EXTERNAL and MANAGED_EXTERNAL are not invited, even when a
    /// directory address matches an anchor domain. INTERNAL stays eligible
    /// when the directory cannot be read.
    #[test]
    fn chat_guest_affiliation_external_is_not_invited() {
        let anchor = one_anchor("coralswarm.test");
        let external = classify_chat_invite_seat(
            "HUMAN",
            "EXTERNAL",
            "users/9",
            "Guest Person",
            Ok(Some("guest@coralswarm.test")),
            &anchor,
        );
        match external {
            ChatInviteSeat::External(Some(record)) => {
                assert_eq!(record.key, "users/9");
                assert!(record.name.is_none(), "a returned displayName is still absent");
                assert!(record.address.is_none(), "a matching directory address is still absent");
            }
            other => panic!("external must not be eligible: {other:?}"),
        }

        let managed = classify_chat_invite_seat(
            "HUMAN",
            "managed_external",
            "users/8",
            "Managed",
            Err(()),
            &[],
        );
        assert!(matches!(managed, ChatInviteSeat::External(Some(record)) if record.key == "users/8"));

        let internal = classify_chat_invite_seat("HUMAN", "INTERNAL", "users/1", "Ada", Err(()), &[]);
        assert_eq!(internal, ChatInviteSeat::Eligible);

        let unknown = classify_chat_invite_seat(
            "HUMAN",
            "GUEST",
            "users/7",
            "X",
            Ok(Some("x@coralswarm.test")),
            &anchor,
        );
        assert_eq!(unknown, ChatInviteSeat::NotInvited, "an unrecognised affiliation fails closed");
    }

    /// Item 42. Directory + domain is only the fallback for a HUMAN row that
    /// lacks `affiliation`. A bot that lacks it is not that fallback.
    #[test]
    fn chat_guest_affiliation_absent_human_uses_directory_and_domain() {
        let anchor = one_anchor("coralswarm.test");
        let admitted = classify_chat_invite_seat(
            "HUMAN",
            "",
            "users/2",
            "Ada",
            Ok(Some("ada@coralswarm.test")),
            &anchor,
        );
        assert_eq!(admitted, ChatInviteSeat::Eligible);

        let other_domain = classify_chat_invite_seat(
            "HUMAN",
            "",
            "users/3",
            "Bea",
            Ok(Some("bea@client.test")),
            &anchor,
        );
        assert_eq!(other_domain, ChatInviteSeat::NotInvited);

        let not_in_directory = classify_chat_invite_seat("HUMAN", "", "users/4", "Cy", Ok(None), &anchor);
        assert_eq!(not_in_directory, ChatInviteSeat::NotInvited);

        let no_anchor = classify_chat_invite_seat(
            "HUMAN",
            "",
            "users/5",
            "Dee",
            Ok(Some("dee@coralswarm.test")),
            &[],
        );
        assert_eq!(no_anchor, ChatInviteSeat::NotInvited);

        let directory_down = classify_chat_invite_seat("HUMAN", "", "users/6", "Ev", Err(()), &anchor);
        assert_eq!(directory_down, ChatInviteSeat::Unavailable);

        let bot = classify_chat_invite_seat("BOT", "", "users/bot", "Bot", Err(()), &[]);
        assert_eq!(bot, ChatInviteSeat::Eligible, "a bot row without affiliation is not the directory fallback");
    }

    /// Item 30. A HUMAN with no affiliation matches any verified org domain.
    /// The connecting member's domain bootstraps only while that list is
    /// empty, and a failed lookup admits nobody.
    #[test]
    fn chat_missing_affiliation_admits_a_later_verified_domain() {
        let anchors = crate::services::org_domains::guest_anchor_domains(
            Ok(vec!["acme.com".into(), "acquired.com".into()]),
            Some("installer.test".into()),
        );
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/later",
                "Pat",
                Ok(Some("pat@acquired.com")),
                &anchors,
            ),
            ChatInviteSeat::Eligible
        );
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/first",
                "Ada",
                Ok(Some("ada@acme.com")),
                &anchors,
            ),
            ChatInviteSeat::Eligible
        );
    }

    #[test]
    fn chat_missing_affiliation_rejects_an_address_on_no_verified_domain() {
        let anchors = crate::services::org_domains::guest_anchor_domains(
            Ok(vec!["acme.com".into(), "acquired.com".into()]),
            Some("installer.test".into()),
        );
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/miss",
                "Kim",
                Ok(Some("kim@client.example")),
                &anchors,
            ),
            ChatInviteSeat::NotInvited
        );
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/boot",
                "Bo",
                Ok(Some("bo@installer.test")),
                &anchors,
            ),
            ChatInviteSeat::NotInvited,
            "the connecting member's domain does not admit once a verified domain exists"
        );
    }

    #[test]
    fn chat_missing_affiliation_bootstraps_only_while_the_verified_list_is_empty() {
        let bootstrap =
            crate::services::org_domains::guest_anchor_domains(Ok(vec![]), Some("installer.test".into()));
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/boot",
                "Bo",
                Ok(Some("bo@installer.test")),
                &bootstrap,
            ),
            ChatInviteSeat::Eligible
        );
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/off",
                "Off",
                Ok(Some("off@client.example")),
                &bootstrap,
            ),
            ChatInviteSeat::NotInvited
        );

        let verified = crate::services::org_domains::guest_anchor_domains(
            Ok(vec!["acme.com".into()]),
            Some("installer.test".into()),
        );
        assert_eq!(
            classify_chat_invite_seat("HUMAN", "", "users/on", "On", Ok(Some("on@acme.com")), &verified),
            ChatInviteSeat::Eligible
        );
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/boot",
                "Bo",
                Ok(Some("bo@installer.test")),
                &verified,
            ),
            ChatInviteSeat::NotInvited
        );
    }

    #[test]
    fn chat_missing_affiliation_failed_lookup_admits_nobody() {
        let failed = crate::services::org_domains::guest_anchor_domains(
            Err("connection reset".into()),
            Some("installer.test".into()),
        );
        assert!(failed.is_empty(), "a failed lookup must not bootstrap");
        assert_eq!(
            classify_chat_invite_seat(
                "HUMAN",
                "",
                "users/fail",
                "Fail",
                Ok(Some("bo@installer.test")),
                &failed,
            ),
            ChatInviteSeat::NotInvited
        );
    }

    /// Item 43. The persisted key is `users/{id}`. A bare id is prefixed.
    /// An empty id is not a key. Name and address stay absent even when
    /// both were supplied.
    #[test]
    fn chat_guest_external_key_is_users_id_and_name_and_address_are_absent() {
        let with_name = external_chat_member_record("users/9", "Guest Person", Some("guest@outside.test"))
            .expect("users/9 is a usable key");
        assert_eq!(with_name.key, "users/9");
        assert!(with_name.name.is_none());
        assert!(with_name.address.is_none());

        let bare = external_chat_member_record("42", "Named", Some("n@outside.test")).unwrap();
        assert_eq!(bare.key, "users/42");
        assert!(bare.name.is_none());
        assert!(bare.address.is_none());

        assert!(external_chat_member_record("users/", "Nobody", None).is_none());
        assert!(external_chat_member_record("  ", "Nobody", None).is_none());
    }

    #[test]
    fn chat_guest_not_invited_does_not_inflate_unmapped() {
        assert_eq!(unmapped_excluding_not_invited(4, 1, 0, 2), 1);
        assert_eq!(unmapped_excluding_not_invited(4, 0, 1, 3), 0);
    }

    #[test]
    fn write_outcome_only_reports_full_success_when_nothing_failed() {
        assert!(WriteOutcome::default().fully_succeeded());
        assert!(!WriteOutcome {
            destinations_failed: 1,
            ..Default::default()
        }
        .fully_succeeded());
        assert!(!WriteOutcome {
            items_failed: 1,
            ..Default::default()
        }
        .fully_succeeded());
    }

    #[test]
    fn seat_source_is_namespaced_and_cannot_collide_with_slack_or_teams() {
        let source = crate::seats::google_chat_space_source("spaces/AAA");
        assert_eq!(source, "google_chat:spaces/AAA");
        assert_ne!(source, crate::seats::slack_channel_source("spaces/AAA"));
        assert_ne!(source, crate::seats::teams_channel_source("spaces/AAA"));
    }

    // =======================================================================
    // pg — every test below runs as the REAL least-privilege `coralswarm_app`
    // role, never as the table owner.
    //
    // This is not a stylistic preference. The superuser/owner connection
    // BYPASSES row-level security, so an RLS-covered assertion made on it
    // passes VACUOUSLY — that shipped a live production defect once (#700)
    // and was re-found in #722. `google_chat_watched_spaces`,
    // `google_watch_channels`, `oauth_tokens`, `atoms`, `graphs`,
    // `team_members` and `team_seat_grants` are ALL RLS-covered, so every
    // test that touches them connects through
    // `crate::app_role_test_support::app_role_pool`.
    // =======================================================================

    use crate::app_role_test_support::app_role_pool;

    /// `(owner pool for DDL/seeding, app-role pool for the code under test,
    /// url)`. Migrations must run as the owner; the code under test must not.
    async fn app_role_db(base: &str, db: &str, tenant_tree: bool) -> (PgPool, PgPool, String) {
        let conn = recreate(base, db).await;
        conn.close().await.ok();
        let url = with_db(base, db);
        let owner: PgPool = PgPoolOptions::new().connect(&url).await.unwrap();
        if tenant_tree {
            sqlx::migrate!("./migrations_tenant").run(&owner).await.unwrap();
        } else {
            sqlx::migrate!("./migrations_control").run(&owner).await.unwrap();
        }
        let app = app_role_pool(&owner, &url).await;
        (owner, app, url)
    }

    async fn drop_db(base: &str, db: &str) {
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// `graphs` INSERT in `resolve_primary_graph`'s own auto-create shape
    /// (#677's fixture-bug class: two separate tests in that PR shipped the
    /// identical bad fixture, and neither caught anything).
    async fn seed_primary_graph(owner: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'My Ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(owner)
        .await
        .unwrap()
    }

    /// A container space's routed destination, read off the table.
    async fn route_graph(owner: &PgPool, account_id: Uuid, space_id: &str) -> Option<Uuid> {
        sqlx::query_scalar(
            "SELECT graph_id FROM google_chat_watched_spaces \
             WHERE account_id = $1 AND space_id = $2 AND owner_user_id IS NULL",
        )
        .bind(account_id)
        .bind(space_id)
        .fetch_one(owner)
        .await
        .unwrap()
    }

    /// Route a container space: `single` to `graph`, or `fanout`.
    async fn seed_space_route(owner: &PgPool, account_id: Uuid, space_id: &str, graph: Option<Uuid>) {
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, mode, graph_id, active) \
             VALUES ($1, $2, 'SPACE', CASE WHEN $3::uuid IS NULL THEN 'fanout' ELSE 'single' END, $3, true)",
        )
        .bind(account_id)
        .bind(space_id)
        .bind(graph)
        .execute(owner)
        .await
        .unwrap();
    }

    async fn seed_member_graph(owner: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary, owner_user_id) \
             VALUES ($1, 'Member Ocean', 'member', true, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(owner)
        .await
        .unwrap()
    }

    async fn seed_team(owner: &PgPool, account_id: Uuid, graph_id: Uuid, created_by: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO teams (account_id, name, graph_id, created_by)              VALUES ($1, 'Eng', $2, $3) RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(created_by)
        .fetch_one(owner)
        .await
        .unwrap()
    }

    /// `atoms` INSERT in `routes::ingest`'s real shape — CHUNK-SUFFIXED
    /// `source_id` (`"{source_id}:chunk:{position}"`), which is what
    /// `ingest_atom`, `routes::slack_events` and this module's own
    /// `ingest_space_items` all actually write. Seeding a BARE `source_id`
    /// here would make every deletion assertion below pass vacuously — the
    /// exact fixture-bug class the design doc's §7 test list calls out by
    /// name, and the reason `propagate_deletions` needs a concrete-id
    /// resolution step at all.
    async fn seed_chunked_atom(owner: &PgPool, graph_id: Uuid, message_name: &str, position: i32) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'google_chat', $2, $3, now()) RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(format!("{message_name}:chunk:{position}"))
        .bind(format!("blob-{position}"))
        .fetch_one(owner)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn chat_watch_row_round_trip_and_a_recreated_subscription_updates_the_same_row_pg() {
        // Design doc §1.2 / risk 8, made falsifiable. `resource_id` = the
        // member's Google `sub` (STABLE across subscription re-creation),
        // `channel_id` = the subscription name (globally unique, which is
        // what `0182:93`'s partial unique index wants). If the two were
        // swapped, a re-created subscription would ACCUMULATE a dead row per
        // lapse instead of updating one — and `renew_chat` would PATCH
        // `v1/users/{sub}` as if it were a subscription name.
        let Some(base) = base_url() else {
            eprintln!("skipping chat_watch_row_round_trip_and_a_recreated_subscription_updates_the_same_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_watch_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let user_id = seed_user(&owner).await;
        let account_id = seed_account(&owner, user_id).await;

        let first_expiry = Utc::now() + chrono::Duration::days(7);
        upsert_watch(
            &app,
            account_id,
            user_id,
            "sub-alice",
            "subscriptions/aaa",
            "projects/p/topics/chat-events",
            first_expiry,
        )
        .await
        .unwrap();

        let rows: Vec<(String, String, String, Option<Uuid>)> = sqlx::query_as(
            "SELECT surface, resource_id, channel_id, owner_user_id FROM google_watch_channels \
             WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_all(&owner)
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "chat");
        assert_eq!(rows[0].1, "sub-alice", "resource_id is the member's Google sub");
        assert_eq!(rows[0].2, "subscriptions/aaa", "channel_id is the subscription name");
        assert_eq!(rows[0].3, Some(user_id), "owner_user_id is the connecting member");

        // A live subscription short-circuits `ensure_subscription` before it
        // ever reaches the network.
        assert!(watch_is_live(&app, account_id, "sub-alice").await.unwrap());
        assert!(
            !watch_is_live(&app, account_id, "sub-someone-else").await.unwrap(),
            "liveness is per MEMBER, not per account"
        );

        // The subscription lapses and is RE-CREATED with a brand-new name.
        let second_expiry = Utc::now() + chrono::Duration::days(7);
        upsert_watch(
            &app,
            account_id,
            user_id,
            "sub-alice",
            "subscriptions/bbb",
            "projects/p/topics/chat-events",
            second_expiry,
        )
        .await
        .unwrap();

        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT resource_id, channel_id FROM google_watch_channels WHERE account_id = $1")
                .bind(account_id)
                .fetch_all(&owner)
                .await
                .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "a RE-CREATED subscription must UPDATE the member's existing row, never add a second \
             one — that is the whole argument for keying on the sub (§1.2)"
        );
        assert_eq!(rows[0].1, "subscriptions/bbb", "the new subscription name replaces the old");

        // A near-expiry row is NOT live, so the next pass re-creates.
        upsert_watch(
            &app,
            account_id,
            user_id,
            "sub-alice",
            "subscriptions/bbb",
            "projects/p/topics/chat-events",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap();
        assert!(
            !watch_is_live(&app, account_id, "sub-alice").await.unwrap(),
            "a subscription inside CHANNEL_LIVE_MARGIN_MINUTES must be treated as not live"
        );

        // An empty `sub` is refused outright — a row keyed on "" would
        // collide across every member of the account.
        let http = reqwest::Client::new();
        assert!(ensure_subscription(&http, &app, account_id, user_id, "  ", "tok").await.is_err());

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn space_cursor_round_trip_and_backfill_arming_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping space_cursor_round_trip_and_backfill_arming_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_space_cursors";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        // CONTROL tree, connected as `coralswarm_app`. The tenant twin's own
        // grants are to `coralswarm_tenant_app` (a different role, covered by
        // `backend/tests/tenant_rls_pg.rs`), and control IS the plane Chat
        // actually runs on until G4.6 lifts the org gate — on the personal
        // plane the control database is the content database.
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let account_id = seed_account(&owner, seed_user(&owner).await).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;
        record_added_to_space(&app, &app, AccountPlane::Personal, account_id, None, &test_space("spaces/C"))
            .await
            .unwrap();

        // A fresh row reads as "nothing pending" — `0198` defaults
        // `backfill_complete` TRUE, matching `gmail_watched_threads`.
        let state = load_space_sync_state(&app, account_id, "spaces/C", None).await.unwrap().unwrap();
        assert!(state.backfill_complete);
        assert_eq!(state.sync_token, None);
        assert_eq!(state.backfill_cursor, None);

        // Routing ARMS the backfill. Without this a routed space would only
        // ever ingest messages posted AFTER the pick, silently breaking
        // §5.4's "auto-seated members read the ocean back to its start".
        pick_space_destination(
            &app,
            &app,
            AccountPlane::Personal,
            account_id,
            "spaces/C",
            PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        assert!(arm_backfill_for_space(&app, account_id, "spaces/C", None).await.unwrap());
        let state = load_space_sync_state(&app, account_id, "spaces/C", None).await.unwrap().unwrap();
        assert!(!state.backfill_complete, "routing arms the backfill");

        // A mid-pass backfill commit persists a resume cursor and stays
        // incomplete.
        commit_pending(
            &app,
            account_id,
            "spaces/C",
            None,
            PendingCommit::Backfill {
                resume_cursor: Some("page-2".into()),
                complete: false,
                seed_watermark: None,
            },
        )
        .await
        .unwrap();
        let state = load_space_sync_state(&app, account_id, "spaces/C", None).await.unwrap().unwrap();
        assert_eq!(state.backfill_cursor.as_deref(), Some("page-2"));
        assert!(!state.backfill_complete);

        // Reaching the terminal page clears the cursor and flips complete.
        commit_pending(
            &app,
            account_id,
            "spaces/C",
            None,
            PendingCommit::Backfill {
                resume_cursor: None,
                complete: true,
                // The handoff seed — see the assertion right below.
                seed_watermark: Some("2026-08-20T00:00:00+00:00".into()),
            },
        )
        .await
        .unwrap();
        let state = load_space_sync_state(&app, account_id, "spaces/C", None).await.unwrap().unwrap();
        assert_eq!(state.backfill_cursor, None);
        assert!(state.backfill_complete);

        // Re-arming a space that has already ingested is REFUSED — a re-pick
        // after a G4-Q6 re-add must not restart a completed backfill.
        assert!(
            !arm_backfill_for_space(&app, account_id, "spaces/C", None).await.unwrap(),
            "a space with a non-NULL last_ingested_at must never be re-armed"
        );

        // Review round 1, H1: completing the backfill SEEDS the catch-up
        // watermark. Without it `sync_token` was still NULL at handoff and
        // `floored_watermark(None, now)` sent every space straight back
        // through a full 28-day `spaceEvents` re-walk of content the backfill
        // had just ingested.
        let after_backfill = load_space_sync_state(&app, account_id, "spaces/C", None).await.unwrap().unwrap();
        assert_eq!(
            after_backfill.sync_token.as_deref(),
            Some("2026-08-20T00:00:00+00:00"),
            "completing a backfill must hand the catch-up path a watermark"
        );

        // Catch-up: a mid-listing pass does NOT advance the watermark, but it
        // DOES persist a resume cursor (review round 1, H1) — without that,
        // a space with more than SYNC_PASS_BUDGET events in its trailing 28
        // days re-listed the identical window every tick, forever.
        commit_pending(
            &app,
            account_id,
            "spaces/C",
            None,
            PendingCommit::CatchUpProgress {
                resume_cursor: Some("events-page-2".into()),
            },
        )
        .await
        .unwrap();
        let mid = load_space_sync_state(&app, account_id, "spaces/C", None).await.unwrap().unwrap();
        assert_eq!(
            mid.sync_token.as_deref(),
            Some("2026-08-20T00:00:00+00:00"),
            "an interrupted catch-up must leave the watermark exactly where it was — advancing \
             past un-ingested events would lose them permanently"
        );
        assert_eq!(
            mid.backfill_cursor.as_deref(),
            Some("events-page-2"),
            "…but it MUST persist where to resume, or the pass never converges"
        );
        assert!(
            mid.backfill_complete,
            "a catch-up resume must not look like an incomplete BACKFILL — the next pass has to \
             re-enter the catch-up path, not restart a backfill"
        );
        commit_pending(
            &app,
            account_id,
            "spaces/C",
            None,
            PendingCommit::CatchUpComplete("2026-08-25T00:00:00+00:00".into()),
        )
        .await
        .unwrap();
        assert_eq!(
            load_space_sync_state(&app, account_id, "spaces/C", None)
                .await
                .unwrap()
                .unwrap()
                .sync_token
                .as_deref(),
            Some("2026-08-25T00:00:00+00:00")
        );

        // `PendingCommit::Nothing` never writes anything.
        commit_pending(&app, account_id, "spaces/C", None, PendingCommit::Nothing).await.unwrap();

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn an_unwatched_unrouted_or_inactive_space_never_ingests_and_never_error_loops_pg() {
        // Design doc §4.3 + §7 correction C. Three distinct "not our
        // business" states, all of which must be quiet no-ops rather than
        // errors: a space with NO row (the ADR-0008 discard — the
        // subscription's reach is deliberately wider than our consent set),
        // an UNROUTED row (`mode IS NULL` — no destination exists, so there
        // is nowhere to put anything), and an INACTIVE row (the app was
        // removed; Slack #502's lesson).
        let Some(base) = base_url() else {
            eprintln!("skipping an_unwatched_unrouted_or_inactive_space_never_ingests_and_never_error_loops_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_discard";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        // CONTROL tree as `coralswarm_app` — see
        // `space_cursor_round_trip_and_backfill_arming_pg`'s own note.
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let account_id = seed_account(&owner, seed_user(&owner).await).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;
        let http = reqwest::Client::new();
        let directory = HashMap::new();

        // 1. No row at all — `fetch_pending` returns Nothing WITHOUT making
        //    a single API call (there is no reachable Chat API from this
        //    harness, so a network attempt would surface as an Err).
        let pending = fetch_pending(&http, &app, account_id, "spaces/never-added", None, "tok", "", &directory, None)
            .await
            .unwrap();
        assert!(pending.items.is_empty());
        assert!(matches!(pending.commit, PendingCommit::Nothing));

        // 2. Unrouted: a row exists but has no destination. It must not be
        //    due for the sweep.
        record_added_to_space(&app, &app, AccountPlane::Personal, account_id, None, &test_space("spaces/U"))
            .await
            .unwrap();
        assert!(
            due_spaces_for_account(&app, account_id, 10).await.unwrap().is_empty(),
            "an UNROUTED space is not due — it has no destination, so it must neither ingest \
             nor error-loop"
        );
        assert!(ingestable_space(&app, account_id, "spaces/U").await.unwrap().is_none());

        // 3. Routed and active: now it is due.
        pick_space_destination(
            &app,
            &app,
            AccountPlane::Personal,
            account_id,
            "spaces/U",
            PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        let due = due_spaces_for_account(&app, account_id, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].space_id, "spaces/U");
        assert_eq!(due[0].mode.as_deref(), Some("single"));
        assert_eq!(route_graph(&owner, account_id, "spaces/U").await, Some(graph_id));
        assert!(ingestable_space(&app, account_id, "spaces/U").await.unwrap().is_some());

        // 4. The app is removed: ingest stops IMMEDIATELY, and the
        //    destination + cursors survive untouched.
        assert!(record_removed_from_space(&app, account_id, "spaces/U").await.unwrap());
        assert!(
            due_spaces_for_account(&app, account_id, 10).await.unwrap().is_empty(),
            "REMOVED_FROM_SPACE must stop ingest immediately (Slack #502's lesson)"
        );
        assert!(ingestable_space(&app, account_id, "spaces/U").await.unwrap().is_none());
        let still_there = list_watched_spaces(&app, account_id).await.unwrap();
        assert_eq!(
            still_there["spaces/U"].graph_id,
            Some(graph_id),
            "deactivation must never discard the human's destination pick"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn c9_delete_removes_every_chunk_and_a_failure_returns_err_so_the_cursor_blocks_pg() {
        // Conformance test 8′, delete half — and the concrete-id resolution
        // design §3.6 did not anticipate. `delete_source` matches
        // `atoms.source_id` EXACTLY (`services/deletion.rs:1416`), while
        // every real ingest path writes `"{source_id}:chunk:{n}"`. The atoms
        // below are seeded in that REAL shape on purpose: with a bare
        // `source_id` this assertion would pass vacuously and C9 would be
        // broken in production while looking green here.
        let Some(base) = base_url() else {
            eprintln!("skipping c9_delete_removes_every_chunk_and_a_failure_returns_err_so_the_cursor_blocks_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_c9";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let user_id = seed_user(&owner).await;
        let account_id = seed_account(&owner, user_id).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        let msg = "spaces/AAA/messages/msg-1";
        let other = "spaces/AAA/messages/msg-2";
        let a0 = seed_chunked_atom(&owner, graph_id, msg, 0).await;
        let a1 = seed_chunked_atom(&owner, graph_id, msg, 1).await;
        let keep = seed_chunked_atom(&owner, graph_id, other, 0).await;

        // The resolution step itself: BOTH chunks of the deleted message,
        // and nothing belonging to any other message.
        let mut resolved = concrete_source_ids(&app, account_id, &[msg.to_string()]).await.unwrap();
        resolved.sort();
        assert_eq!(
            resolved,
            vec![format!("{msg}:chunk:0"), format!("{msg}:chunk:1")],
            "every chunk of the deleted message must be resolved — handing delete_source the \
             bare message name matches ZERO rows"
        );

        propagate_deletions(&app, account_id, graph_id, &[msg.to_string()])
            .await
            .unwrap();

        for gone in [a0, a1] {
            let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
                .bind(gone)
                .fetch_one(&owner)
                .await
                .unwrap();
            assert_eq!(n, 0, "an upstream delete must remove every chunk of that message");
        }
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(keep)
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(n, 1, "a sibling message must be untouched");

        // A deletion for a message this account never ingested is a correct,
        // quiet no-op — never an error, so it can never block a cursor.
        propagate_deletions(&app, account_id, graph_id, &["spaces/AAA/messages/never".to_string()])
            .await
            .unwrap();
        assert!(propagate_deletions(&app, account_id, graph_id, &[]).await.is_ok());

        // The blocking half (#686 round 2): a resolution failure must return
        // `Err`, so `fetch_pending`'s `?` leaves the cursor unchanged and
        // the same range is re-diffed next pass. Simulated by an anchor
        // graph that belongs to NO account — `delete_source` then resolves
        // no account and the whole batch aborts rather than silently
        // "succeeding" with nothing deleted.
        let msg3 = "spaces/AAA/messages/msg-3";
        let a3 = seed_chunked_atom(&owner, graph_id, msg3, 0).await;
        sqlx::query("DROP TABLE IF EXISTS atoms_backup").execute(&owner).await.ok();
        // Force the batch lookup itself to fail by revoking the app role's
        // read on `atoms` for the duration of one call.
        sqlx::query("REVOKE SELECT ON atoms FROM coralswarm_app").execute(&owner).await.unwrap();
        let blocked = propagate_deletions(&app, account_id, graph_id, &[msg3.to_string()]).await;
        sqlx::query("GRANT SELECT ON atoms TO coralswarm_app").execute(&owner).await.unwrap();
        assert!(
            blocked.is_err(),
            "a deletion-propagation failure MUST return Err — the caller's `?` is what leaves the \
             cursor unchanged, and a Chat deletion is reported exactly once per event range"
        );
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(a3)
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(n, 1, "nothing was deleted on the aborted batch");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn c9_an_upstream_edit_never_mutates_an_already_ingested_atom_pg() {
        // Conformance test 8′, EDIT half (§5.3, founder answer Q4). The
        // declaration is "deletions are honoured; edits made after ingest
        // are not reflected". This pins it so it cannot drift into an
        // implied guarantee: `message.v1.updated` is not subscribed
        // (asserted purely above), and nothing in this module's ingest path
        // rewrites an existing atom — the write is
        // `ON CONFLICT (graph_id, source_type, source_id) DO NOTHING`, so a
        // re-ingest of an EDITED message leaves the original content in
        // place rather than silently mutating it.
        let Some(base) = base_url() else {
            eprintln!("skipping c9_an_upstream_edit_never_mutates_an_already_ingested_atom_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_edit";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let user_id = seed_user(&owner).await;
        let account_id = seed_account(&owner, user_id).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        // The fixture's edited message — `lastUpdateTime` set.
        const EDITED: &str = include_str!("../../tests/fixtures/google_chat/message_get.json");
        let edited: ChatMessage = serde_json::from_str(EDITED).unwrap();
        assert!(
            edited.last_update_time.is_some(),
            "message_get.json is the EDITED case — a non-empty lastUpdateTime is the whole point"
        );

        let source_id = format!("{}:chunk:0", edited.name);
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, timestamp) \
             VALUES ($1, 'google_chat', $2, 'blob-0', 'the ORIGINAL text', now())",
        )
        .bind(graph_id)
        .bind(&source_id)
        .execute(&owner)
        .await
        .unwrap();

        // Re-ingesting the edited message: the connector's own INSERT shape.
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&app, account_id)
            .await
            .unwrap();
        let inserted: Option<Uuid> = sqlx::query_scalar(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, timestamp) \
             VALUES ($1, 'google_chat', $2, 'blob-0', 'the EDITED text', now()) \
             ON CONFLICT (graph_id, source_type, source_id) DO NOTHING RETURNING atom_id",
        )
        .bind(graph_id)
        .bind(&source_id)
        .fetch_optional(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(inserted.is_none(), "a re-ingest of an edited message inserts nothing new");

        let content: String = sqlx::query_scalar("SELECT content_inline FROM atoms WHERE source_id = $1")
            .bind(&source_id)
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(
            content, "the ORIGINAL text",
            "an upstream EDIT must not silently mutate ingested content — §5.3 declares edits \
             unreflected in v1, and this is what stops that declaration drifting"
        );

        // But a DELETE of the same message still removes it — the two halves
        // of the C9 declaration, together.
        propagate_deletions(&app, account_id, graph_id, &[edited.name.clone()])
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE source_id = $1")
            .bind(&source_id)
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(n, 0, "deletions ARE honoured (C9 = yes)");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    /// **Finding 2, review round 2.** The "a deletion we cannot even attempt
    /// is RETRYABLE, so nothing is committed" path used to return
    /// `PendingCommit::CatchUpProgress { resume_cursor: None }`, which
    /// commits TWICE over: `upsert_space_sync_columns` writes
    /// `backfill_cursor = $4` unconditionally (so `None` ERASES a stored
    /// catch-up resume cursor) and stamps `last_ingested_at = now()`.
    ///
    /// Both halves are asserted here, and the old value is exercised on a
    /// SECOND row as an in-test negative control — so this test documents
    /// precisely why `Nothing` is required and goes red the moment anyone
    /// "simplifies" the branch back.
    #[tokio::test]
    async fn un_propagatable_deletion_commit_writes_nothing_at_all_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping un_propagatable_deletion_commit_writes_nothing_at_all_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_nothing_commit";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let user_id = seed_user(&owner).await;
        let account_id = seed_account(&owner, user_id).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        // Two identically-prepared rows: a mid-catch-up space carrying a
        // watermark, an in-flight resume cursor, and a freshness stamp.
        for space in ["spaces/KEEP", "spaces/CTRL"] {
            record_added_to_space(&app, &app, AccountPlane::Personal, account_id, None, &test_space(space))
                .await
                .unwrap();
            pick_space_destination(
                &app,
                &app,
                AccountPlane::Personal,
                account_id,
                space,
                PickedDestination::Single(graph_id),
            )
            .await
            .unwrap();
            commit_pending(&app, account_id, space, None, PendingCommit::CatchUpComplete("2026-08-01T00:00:00+00:00".into()))
                .await
                .unwrap();
            commit_pending(
                &app,
                account_id,
                space,
                None,
                PendingCommit::CatchUpProgress {
                    resume_cursor: Some("page-7".into()),
                },
            )
            .await
            .unwrap();
        }

        // Read `last_ingested_at` too — `load_space_sync_state` does not
        // expose it, and it is half the bug (an operator reads it to decide
        // whether a space is healthy).
        async fn snapshot(
            pool: &PgPool,
            account_id: Uuid,
            space_id: &str,
        ) -> (Option<String>, Option<String>, bool, Option<chrono::DateTime<chrono::Utc>>) {
            sqlx::query_as(
                "SELECT sync_token, backfill_cursor, backfill_complete, last_ingested_at \
                 FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = $2",
            )
            .bind(account_id)
            .bind(space_id)
            .fetch_one(pool)
            .await
            .unwrap()
        }

        let before_keep = snapshot(&owner, account_id, "spaces/KEEP").await;
        let before_ctrl = snapshot(&owner, account_id, "spaces/CTRL").await;
        assert_eq!(before_keep.0.as_deref(), Some("2026-08-01T00:00:00+00:00"));
        assert_eq!(before_keep.1.as_deref(), Some("page-7"));
        assert!(before_keep.3.is_some(), "fixture must actually have a freshness stamp to lose");

        // Sleep so a `now()` stamp would be DISTINGUISHABLE from the one
        // already stored. Without this the `last_ingested_at` assertion
        // could pass vacuously inside one statement_timestamp() tick.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // THE PATH UNDER TEST — the exact value the un-propagatable-deletion
        // branch of `fetch_pending` returns.
        commit_pending(&app, account_id, "spaces/KEEP", None, un_propagatable_deletion_commit())
            .await
            .unwrap();
        let after_keep = snapshot(&owner, account_id, "spaces/KEEP").await;
        assert_eq!(
            after_keep, before_keep,
            "a pass that could not even attempt its deletions must commit NOTHING: the \
             watermark, the catch-up resume cursor, backfill_complete and the last_ingested_at \
             freshness stamp must all be byte-identical"
        );

        // NEGATIVE CONTROL — the value this branch used to return. Proves the
        // test is not vacuous and names the damage precisely.
        commit_pending(
            &app,
            account_id,
            "spaces/CTRL",
            None,
            PendingCommit::CatchUpProgress { resume_cursor: None },
        )
        .await
        .unwrap();
        let after_ctrl = snapshot(&owner, account_id, "spaces/CTRL").await;
        assert_ne!(after_ctrl, before_ctrl, "the old value must be observably damaging, or this test proves nothing");
        assert_eq!(
            after_ctrl.1, None,
            "`CatchUpProgress {{ resume_cursor: None }}` ERASES the stored resume cursor — \
             `backfill_cursor = $4` is unconditional, not COALESCEd"
        );
        assert!(
            after_ctrl.3 > before_ctrl.3,
            "...and it stamps last_ingested_at, so a space that is silently losing deletions \
             reports itself as freshly ingested"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn reader_election_fails_over_to_another_connected_member_then_orphans_pg() {
        // Design doc §3.5/§1.8: failover is a RE-ELECTION, not a row
        // deletion. The destination and every cursor must survive both a
        // failover and the "nobody left" end state.
        let Some(base) = base_url() else {
            eprintln!("skipping reader_election_fails_over_to_another_connected_member_then_orphans_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_reader";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let bob = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;
        seed_oauth_token(&owner, account_id, alice, "google_chat", "sub-alice").await;
        seed_oauth_token(&owner, account_id, bob, "google_chat", "sub-bob").await;
        // Both are CURRENT members. `elect_space_reader` gates on
        // `account_members` (security addendum, H1) — an ex-member's
        // surviving token is not a candidate, so a fixture that seeds only
        // `oauth_tokens` now elects nobody. Pinned separately by
        // `an_offboarded_ex_member_is_never_elected_as_a_space_reader_pg`.
        for user_id in [alice, bob] {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account_id)
                .bind(user_id)
                .execute(&owner)
                .await
                .unwrap();
        }

        record_added_to_space(&app, &app, AccountPlane::Personal, account_id, Some(alice), &test_space("spaces/R"))
            .await
            .unwrap();
        pick_space_destination(
            &app,
            &app,
            AccountPlane::Personal,
            account_id,
            "spaces/R",
            PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        commit_pending(
            &app,
            account_id,
            "spaces/R",
            None,
            PendingCommit::CatchUpComplete("2026-08-01T00:00:00+00:00".into()),
        )
        .await
        .unwrap();

        // Election excludes a named member — a failover never re-elects the
        // reader that just failed.
        assert_eq!(elect_space_reader(&app, account_id, &[alice]).await.unwrap(), Some(bob));
        assert!(elect_space_reader(&app, account_id, &[]).await.unwrap().is_some());

        // Alice disconnects: her token row goes, and the space re-elects
        // onto Bob.
        sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(alice)
            .execute(&owner)
            .await
            .unwrap();
        let (reelected, orphaned) = reelect_reader_for_departing_member(&app, &app, account_id, alice)
            .await
            .unwrap();
        assert_eq!((reelected, orphaned), (1, 0));
        let row = ingestable_space(&app, account_id, "spaces/R").await.unwrap().unwrap();
        assert_eq!(row.reader_user_id, Some(bob));
        assert_eq!(route_graph(&owner, account_id, "spaces/R").await, Some(graph_id), "failover must not disturb the destination");

        // Bob disconnects too: nobody is left. The row keeps its
        // destination AND its cursor, and simply has no reader — the
        // correct end state, not an error and not a terminal row.
        sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(bob)
            .execute(&owner)
            .await
            .unwrap();
        let (reelected, orphaned) = reelect_reader_for_departing_member(&app, &app, account_id, bob)
            .await
            .unwrap();
        assert_eq!((reelected, orphaned), (0, 1));
        let row = ingestable_space(&app, account_id, "spaces/R").await.unwrap().unwrap();
        assert_eq!(row.reader_user_id, None);
        assert_eq!(route_graph(&owner, account_id, "spaces/R").await, Some(graph_id), "\"nobody left\" must never discard the pick");
        assert_eq!(
            load_space_sync_state(&app, account_id, "spaces/R", None)
                .await
                .unwrap()
                .unwrap()
                .sync_token
                .as_deref(),
            Some("2026-08-01T00:00:00+00:00"),
            "\"nobody left\" must never discard the cursor either — ingest resumes exactly where \
             it stopped the moment anybody reconnects"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn seating_over_an_org_fixture_at_n2_with_the_gate_closed_pg() {
        // **Conformance test 4 (I4), design doc §6.** An org fixture at
        // N = 2 with `ORG_GOOGLE_CHAT_ENABLED` never consulted — D1 keeps
        // the org gate closed until G4.6, so this exact behaviour is
        // UN-EXERCISABLE IN PRODUCTION until then. That is stated in the PR
        // body; it is also exactly why the design requires the seating code
        // to ship plane-generically and be pg-tested here rather than
        // waiting for the lift.
        //
        // Routing a space to a shared ocean seats every JOINED, non-bot,
        // MATCHED roster member — and nobody else. The roster read itself
        // needs a live Chat API this crate has no mock harness for (the same
        // constraint `connectors::slack`'s own tests document), so this
        // drives the two halves the API does not gate: the identity+
        // membership matching (`matched_members_for_roster`) over the real
        // 7-member fixture, and the seat write.
        let Some(base) = base_url() else {
            eprintln!("skipping seating_over_an_org_fixture_at_n2_with_the_gate_closed_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_seating";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let bob = seed_user(&owner).await;
        let stranger = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        // N = 2 org members. `users/1001` (Alice) and `users/1002` (Bob) are
        // the two the fixture README designates as mappable; `users/1003`
        // and `users/1004` are deliberately left unmapped.
        for (user_id, sub) in [(alice, "1001"), (bob, "1002")] {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account_id)
                .bind(user_id)
                .execute(&owner)
                .await
                .unwrap();
            seed_oauth_token(&owner, account_id, user_id, "google_chat", sub).await;
        }
        // A THIRD person who has connected Google under this account but is
        // NOT an `account_members` row — the gate must exclude them, and a
        // DB error on that gate must propagate rather than silently reading
        // as "not a member" (`slack.rs:1461`-`:1468`).
        seed_oauth_token(&owner, account_id, stranger, "google_chat", "1003").await;

        let roster = fixture_roster();
        // No live Chat/People API mock harness exists in this crate (this
        // test's own doc comment, above) — `directory = Err(..)` exercises
        // exactly the "domain directory unavailable this pass" branch, and
        // the assertion below proves that branch does NOT change this
        // test's expected outcome: `users/1004` (Priya, no oauth_tokens row)
        // was ALREADY unmatched via the sub path before this PR, and stays
        // unmatched — as `Unavailable`, never as a false `Matched` — when
        // the directory fallback cannot be asked either.
        let roster_outcome = matched_members_for_roster(
            &app,
            &app,
            account_id,
            Some("gws-g44-seating-test-workspace"),
            &roster,
            Err("no live People API mock harness in this test"),
            &[],
        )
        .await
        .unwrap();
        let matched = roster_outcome.matched;
        let mut matched_sorted = matched.clone();
        matched_sorted.sort();
        let mut expected = vec![alice, bob];
        expected.sort();
        assert_eq!(
            matched_sorted, expected,
            "N = matched identities that ALSO pass EXISTS(account_members) — a connected \
             non-member is never seated"
        );
        assert_eq!(
            roster_outcome.unavailable, 1,
            "users/1004 (Priya) fails the sub match AND the directory itself is Err this test — \
             that is `unavailable` (could not check), never folded into a false `Unmatched`/\
             `unmapped` count. users/1003 (the stranger) DOES sub-match — it fails the \
             account_members gate afterward, which is a different, unaffected code path."
        );

        let team_id: Uuid = seed_team(&owner, account_id, graph_id, alice).await;

        let source = crate::seats::google_chat_space_source("spaces/AAAAAAAAAAA");
        // Founding pass: full history (`None`) — §5.4's promise that
        // auto-seated members read the ocean back to its start.
        let history_from = crate::seats::founding_history_from(&app, account_id, team_id, &source)
            .await
            .unwrap();
        assert!(
            history_from.is_none(),
            "the FOUNDING roster of a newly routed space gets FULL history — withholding it \
             would hide the very backfill this routing action just triggered from the people \
             who wrote it"
        );
        let seated = crate::routes::slack_interactivity::seat_members_for_source(
            &app,
            account_id,
            team_id,
            &matched,
            &source,
            history_from,
        )
        .await;
        assert_eq!(seated, 2, "both matched members are seated");
        crate::seats::record_source_founding(&app, account_id, team_id, &source)
            .await
            .unwrap();

        // Nobody's content lands where they cannot read it: exactly the two
        // matched members hold seats, and every seat is `role = 'member'` —
        // auto-seating grants read access, never administration.
        let rows: Vec<(Uuid, String)> =
            sqlx::query_as("SELECT user_id, role FROM team_members WHERE team_id = $1 ORDER BY user_id")
                .bind(team_id)
                .fetch_all(&owner)
                .await
                .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(_, role)| role == "member"));
        let mut seated_ids: Vec<Uuid> = rows.iter().map(|(u, _)| *u).collect();
        seated_ids.sort();
        assert_eq!(seated_ids, expected);

        // Conformance test 5's STAMPING half (the ENFORCEMENT half — that a
        // windowed member genuinely cannot read pre-window atoms on the live
        // retrieval path — is already pinned mechanism-wide by
        // `backend/tests/history_from_pg.rs`, which ships §6's tests #5 and
        // #6 against the real `coralswarm_tenant_app` role; duplicating it
        // per connector would test m365 Train 4.1's code, not this PR's).
        // A LATE joiner discovered by a later sweep is stamped `Some(now)`.
        let late_history_from = crate::seats::founding_history_from(&app, account_id, team_id, &source)
            .await
            .unwrap();
        assert!(
            late_history_from.is_some(),
            "once a source is founded, anyone discovered later is a LATE JOINER and must not \
             receive full history"
        );
        let carol = seed_user(&owner).await;
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(account_id)
            .bind(carol)
            .execute(&owner)
            .await
            .unwrap();
        crate::routes::slack_interactivity::seat_members_for_source(
            &app,
            account_id,
            team_id,
            &[carol],
            &source,
            late_history_from,
        )
        .await;
        let carol_floor: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT history_from FROM team_seat_grants WHERE team_id = $1 AND user_id = $2 AND source = $3",
        )
        .bind(team_id)
        .bind(carol)
        .bind(&source)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert!(carol_floor.is_some(), "a late joiner's grant carries a window floor");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    // ── Domain directory (People API) — C5 verified-email matching ─────────
    // (chat-per-user-setup PR, 2026-09-20). No live Chat/People API mock
    // harness exists in this crate (same constraint every other `_pg` test
    // in this file documents), so these tests drive
    // `resolve_chat_directory_identity`/`match_and_persist_chat_verified_
    // email`/`persist_chat_canonical_name` directly with a hand-built
    // directory map, exactly the way `matched_members_for_roster`'s own
    // caller ([`auto_seat_space_members`]) would after a real fetch.

    #[tokio::test]
    async fn chat_directory_identity_matches_and_writes_the_verified_link_half_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping chat_directory_identity_matches_and_writes_the_verified_link_half_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_chat_directory_matched";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let priya = seed_user_with_email(&owner, "priya.raman@k12isd.example").await;
        let account_owner = seed_user(&owner).await;
        let account_id = seed_account(&owner, account_owner).await;

        let mut directory = HashMap::new();
        directory.insert(
            "1004".to_string(),
            DirectoryEntry {
                email: "priya.raman@k12isd.example".to_string(),
                display_name: "Priya Raman".to_string(),
            },
        );

        let outcome = resolve_chat_directory_identity(
            &app,
            &app,
            account_id,
            "reader@k12isd.example",
            "1004",
            "Priya Raman",
            Ok(&directory),
        )
        .await;
        assert_eq!(
            outcome,
            ChatVerifiedIdentity::Matched(priya),
            "an exact-normalised email match against a live `users` row must MATCH and return \
             that user's id"
        );

        let row: (Option<Uuid>, Option<String>, bool, String) = sqlx::query_as(
            "SELECT user_id, link_method, link_verified_at IS NOT NULL, canonical_name \
             FROM connector_identities WHERE provider = 'google_chat' AND workspace_id = $1 \
               AND external_user_id = $2",
        )
        .bind("reader@k12isd.example")
        .bind("1004")
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(row.0, Some(priya), "the verified-link half must be written on a confirmed match");
        assert_eq!(
            row.1.as_deref(),
            Some("verified_email"),
            "link_method must be exactly 'verified_email' — the only value migration 0104's CHECK \
             permits; no widened CHECK, no invented 'google_sub' method"
        );
        assert!(row.2, "link_verified_at must be set on a confirmed link");
        assert_eq!(row.3, "Priya Raman");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn chat_directory_identity_absent_member_is_genuinely_unmatched_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping chat_directory_identity_absent_member_is_genuinely_unmatched_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_chat_directory_unmatched";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;
        let account_owner = seed_user(&owner).await;
        let account_id = seed_account(&owner, account_owner).await;

        // A SUCCESSFULLY fetched directory that simply does not list this
        // sub (an external guest, e.g.) — a real answer about this one
        // person, not a failure.
        let directory: HashMap<String, DirectoryEntry> = HashMap::new();
        let outcome = resolve_chat_directory_identity(
            &app,
            &app,
            account_id,
            "reader@k12isd.example",
            "9999-guest",
            "External Guest",
            Ok(&directory),
        )
        .await;
        assert_eq!(outcome, ChatVerifiedIdentity::Unmatched);

        let has_link: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM connector_identities WHERE provider = 'google_chat' \
               AND workspace_id = $1 AND external_user_id = $2 AND user_id IS NOT NULL)",
        )
        .bind("reader@k12isd.example")
        .bind("9999-guest")
        .fetch_one(&owner)
        .await
        .unwrap();
        assert!(!has_link, "an Unmatched outcome must never write a verified link");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn chat_directory_identity_is_unavailable_when_the_directory_failed_never_unmatched_pg() {
        // **This is the one that matters most** (task/coordinator emphasis):
        // an empty or failed directory must read as `Unavailable`, never as
        // `Unmatched` — the exact conflation that, one connector over for
        // Slack, would have emailed entire workspaces (PR 1, Slack OAuth
        // scope reconciliation).
        let Some(base) = base_url() else {
            eprintln!(
                "skipping chat_directory_identity_is_unavailable_when_the_directory_failed_never_unmatched_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_chat_directory_unavailable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;
        let account_owner = seed_user(&owner).await;
        let account_id = seed_account(&owner, account_owner).await;

        // Seed a `users` row with an address that WOULD have matched, to
        // prove the failure short-circuits BEFORE any lookup is attempted —
        // a directory we could not read must never accidentally still
        // resolve.
        let _would_have_matched = seed_user_with_email(&owner, "priya.raman@k12isd.example").await;

        let outcome = resolve_chat_directory_identity(
            &app,
            &app,
            account_id,
            "reader@k12isd.example",
            "1004",
            "Priya Raman",
            Err("simulated: domain admin has not enabled directory sharing"),
        )
        .await;
        assert!(
            matches!(outcome, ChatVerifiedIdentity::Unavailable(_)),
            "a directory that could not be established must be Unavailable, not Unmatched — got \
             {outcome:?}; reporting it as Unmatched is how a scope-broken or sharing-disabled \
             domain would be silently read as \"nobody here has a CoralSwarm account\""
        );
        assert_ne!(outcome, ChatVerifiedIdentity::Unmatched, "the one distinction that matters most");

        let row_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM connector_identities WHERE provider = 'google_chat' \
               AND workspace_id = $1 AND external_user_id = $2)",
        )
        .bind("reader@k12isd.example")
        .bind("1004")
        .fetch_one(&owner)
        .await
        .unwrap();
        assert!(
            !row_exists,
            "an Unavailable outcome must write NOTHING at all — it never got as far as reading \
             the directory, so there is nothing legitimate to persist"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn chat_directory_identity_email_alias_does_not_match_exact_only_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping chat_directory_identity_email_alias_does_not_match_exact_only_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_chat_directory_alias";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;
        let account_owner = seed_user(&owner).await;
        let account_id = seed_account(&owner, account_owner).await;
        let _base_user = seed_user_with_email(&owner, "priya.raman@k12isd.example").await;

        // The directory hands back a `+tag` alias of the SAME mailbox — must
        // NOT match (`connectors::slack::normalize_email`'s own contract:
        // lowercase + trim ONLY, no provider canonicalisation, reused
        // verbatim by this connector rather than re-implemented).
        let mut directory = HashMap::new();
        directory.insert(
            "1004".to_string(),
            DirectoryEntry {
                email: "priya.raman+chat@k12isd.example".to_string(),
                display_name: "Priya Raman".to_string(),
            },
        );
        let outcome = resolve_chat_directory_identity(
            &app,
            &app,
            account_id,
            "reader@k12isd.example",
            "1004",
            "Priya Raman",
            Ok(&directory),
        )
        .await;
        assert_eq!(
            outcome,
            ChatVerifiedIdentity::Unmatched,
            "a +tag alias is a DIFFERENT address and must not match the base mailbox"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn persist_chat_canonical_name_never_clobbers_an_earlier_confirmed_link_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping persist_chat_canonical_name_never_clobbers_an_earlier_confirmed_link_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_chat_canonical_name_safe";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;
        let priya = seed_user_with_email(&owner, "priya.raman@k12isd.example").await;
        let account_owner = seed_user(&owner).await;
        let account_id = seed_account(&owner, account_owner).await;

        // Confirm a link first — mirrors a pass where the directory matched.
        let user_id = match_and_persist_chat_verified_email(
            &app,
            &app,
            account_id,
            "reader@k12isd.example",
            "1004",
            "priya.raman@k12isd.example",
            "Priya Raman",
        )
        .await
        .unwrap();
        assert_eq!(user_id, Some(priya));

        // A LATER pass writes only the plain canonical name (e.g. the
        // roster's display name changed, or the directory was unavailable
        // that pass) — this must NOT null out the link fields.
        persist_chat_canonical_name(&app, account_id, "reader@k12isd.example", "1004", "Priya R.")
            .await
            .unwrap();

        let row: (Option<Uuid>, Option<String>, bool, String) = sqlx::query_as(
            "SELECT user_id, link_method, link_verified_at IS NOT NULL, canonical_name \
             FROM connector_identities WHERE provider = 'google_chat' AND workspace_id = $1 \
               AND external_user_id = $2",
        )
        .bind("reader@k12isd.example")
        .bind("1004")
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(row.0, Some(priya), "a plain canonical-name upsert must never clear a CONFIRMED link");
        assert_eq!(row.1.as_deref(), Some("verified_email"));
        assert!(row.2);
        assert_eq!(row.3, "Priya R.", "canonical_name itself DOES stay fresh");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn departure_revokes_only_what_this_space_granted_and_never_a_preexisting_seat_pg() {
        // **Conformance test 7 (Departure), design doc §6.**
        let Some(base) = base_url() else {
            eprintln!("skipping departure_revokes_only_what_this_space_granted_and_never_a_preexisting_seat_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_departure";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let bob = seed_user(&owner).await;
        let hand_granted = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;
        let team_id: Uuid = seed_team(&owner, account_id, graph_id, alice).await;

        let space_a = "spaces/AAA";
        let space_b = "spaces/BBB";
        let source_a = crate::seats::google_chat_space_source(space_a);
        let source_b = crate::seats::google_chat_space_source(space_b);

        // Alice is seated by BOTH spaces; Bob only by A.
        crate::routes::slack_interactivity::seat_members_for_source(&app, account_id, team_id, &[alice, bob], &source_a, None).await;
        crate::routes::slack_interactivity::seat_members_for_source(&app, account_id, team_id, &[alice], &source_b, None).await;
        // A hand-granted seat with NO provenance at all — the `preexisting`
        // marker is what stops this system ever revoking it.
        sqlx::query("INSERT INTO team_members(team_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(team_id)
            .bind(hand_granted)
            .execute(&owner)
            .await
            .unwrap();
        crate::routes::slack_interactivity::seat_members_for_source(&app, account_id, team_id, &[hand_granted], &source_a, None).await;

        // Alice leaves space A: the grant drops, the SEAT stands (space B
        // still justifies it).
        let outcome = revoke_space_seat(&app, account_id, team_id, alice, space_a).await.unwrap();
        assert!(matches!(outcome, crate::seats::RevokeOutcome::GrantDropped { .. }));
        let still: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM team_members WHERE team_id = $1 AND user_id = $2 AND departing_at IS NULL",
        )
        .bind(team_id)
        .bind(alice)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(still, 1, "a seat survives while ANY grant survives");

        // Bob leaves space A: it was his only reason, so the seat goes.
        let outcome = revoke_space_seat(&app, account_id, team_id, bob, space_a).await.unwrap();
        assert_eq!(outcome, crate::seats::RevokeOutcome::SeatRevoked);

        // The hand-granted seat's `preexisting` marker survives, so leaving
        // space A drops only the chat grant and never the seat.
        let outcome = revoke_space_seat(&app, account_id, team_id, hand_granted, space_a).await.unwrap();
        assert!(
            matches!(outcome, crate::seats::RevokeOutcome::GrantDropped { .. }),
            "a seat we found rather than created is NOT ours to revoke"
        );

        // A departure we never granted for is `NotOurs` — never a silent
        // revocation of somebody else's access.
        let outcome = revoke_space_seat(&app, account_id, team_id, alice, "spaces/never-routed")
            .await
            .unwrap();
        assert_eq!(outcome, crate::seats::RevokeOutcome::NotOurs);

        // And it revoked what `'google_chat:{space}'` granted and NOTHING
        // else: a Slack grant on the same team for the same person survives a
        // Chat departure untouched. A fresh member is used here rather than
        // one of the people above, so the assertion is about the SOURCE
        // namespacing and nothing else.
        let dave = seed_user(&owner).await;
        let slack_source = crate::seats::slack_channel_source("C123");
        crate::routes::slack_interactivity::seat_members_for_source(&app, account_id, team_id, &[dave], &source_a, None).await;
        crate::routes::slack_interactivity::seat_members_for_source(&app, account_id, team_id, &[dave], &slack_source, None).await;
        let before: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM team_seat_grants WHERE team_id = $1 AND user_id = $2 AND source = $3",
        )
        .bind(team_id)
        .bind(dave)
        .bind(&slack_source)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(before, 1, "the Slack grant was recorded");

        let outcome = revoke_space_seat(&app, account_id, team_id, dave, space_a).await.unwrap();
        assert!(
            matches!(outcome, crate::seats::RevokeOutcome::GrantDropped { .. }),
            "the Slack grant still justifies the seat, so only the Chat grant drops"
        );
        let after: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM team_seat_grants WHERE team_id = $1 AND user_id = $2 AND source = $3",
        )
        .bind(team_id)
        .bind(dave)
        .bind(&slack_source)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(after, 1, "a Chat departure must never touch a Slack grant");
        let chat_grant: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM team_seat_grants WHERE team_id = $1 AND user_id = $2 AND source = $3",
        )
        .bind(team_id)
        .bind(dave)
        .bind(&source_a)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(chat_grant, 0, "the Chat grant for this space IS dropped");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn fanout_destinations_are_member_scoped_and_unmapped_is_derived_pg() {
        // **Conformance test 3 (no consumer-plane fallback), design doc §6**,
        // read through the destination resolver: a fan-out resolves ONLY to
        // oceans belonging to CURRENT members of THIS account, never to the
        // shared plane and never across accounts. Plus §1.10 lesson 1's
        // derived-`unmapped` rule.
        let Some(base) = base_url() else {
            eprintln!("skipping fanout_destinations_are_member_scoped_and_unmapped_is_derived_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_fanout";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let bob = seed_user(&owner).await;
        let no_ocean = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;

        // A WHOLLY UNRELATED account whose member is also connected — must
        // never appear in this account's fan-out.
        let outsider = seed_user(&owner).await;
        let other_account = seed_account(&owner, outsider).await;
        seed_oauth_token(&owner, other_account, outsider, "google_chat", "sub-outsider").await;
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(other_account)
            .bind(outsider)
            .execute(&owner)
            .await
            .unwrap();
        seed_member_graph(&owner, other_account, outsider).await;

        for (user_id, sub) in [(alice, "sub-alice"), (bob, "sub-bob"), (no_ocean, "sub-no-ocean")] {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account_id)
                .bind(user_id)
                .execute(&owner)
                .await
                .unwrap();
            seed_oauth_token(&owner, account_id, user_id, "google_chat", sub).await;
        }
        let alice_ocean = seed_member_graph(&owner, account_id, alice).await;
        let bob_ocean = seed_member_graph(&owner, account_id, bob).await;

        let crate::connectors::slack::FanOut { mut graphs, unmapped_members: unmapped, .. } =
            resolve_chat_fanout_destinations(&app, &app, account_id).await.unwrap();
        graphs.sort();
        let mut expected = vec![alice_ocean, bob_ocean];
        expected.sort();
        assert_eq!(
            graphs, expected,
            "a fan-out resolves ONLY to member oceans of THIS account — never the shared plane, \
             never another account's"
        );
        assert_eq!(
            unmapped, 1,
            "unmapped is DERIVED as total_members - mapped_members, and the member with no \
             provisioned ocean is counted, never guessed at"
        );

        // A member REMOVED from the account drops out immediately — the
        // read-order property (identities first, members last) means a
        // just-removed member can never receive content.
        sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(bob)
            .execute(&owner)
            .await
            .unwrap();
        let crate::connectors::slack::FanOut { graphs, .. } = resolve_chat_fanout_destinations(&app, &app, account_id).await.unwrap();
        assert_eq!(
            graphs, vec![alice_ocean],
            "a member removed from the account must lose access immediately, not at the next \
             cache expiry"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn fanout_destinations_also_match_via_the_domain_directory_verified_link_pg() {
        // Coordinator review (chat-per-user-setup PR, 2026-09-20): fan-out
        // is the path that decides whose ocean content actually lands in,
        // so a colleague resolved ONLY through the domain-directory
        // verified-email match (never having connected Google themselves)
        // must be reachable here too — the exact gap `resolve_chat_
        // fanout_destinations`'s stale doc comment used to make impossible
        // ("no email-keyed directory row to intersect against").
        let Some(base) = base_url() else {
            eprintln!(
                "skipping fanout_destinations_also_match_via_the_domain_directory_verified_link_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g44_fanout_directory";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await; // sub-matched (connected Google herself)
        let priya = seed_user(&owner).await; // NEVER connected Google — directory-matched only
        let account_id = seed_account(&owner, alice).await;

        for user_id in [alice, priya] {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account_id)
                .bind(user_id)
                .execute(&owner)
                .await
                .unwrap();
        }
        // Alice connected Google herself — the pre-existing sub-match path,
        // and ALSO the anchor `account_chat_workspace_ids` scopes the
        // directory read on.
        seed_oauth_token(&owner, account_id, alice, "google_chat", "sub-alice").await;
        let alice_ocean = seed_member_graph(&owner, account_id, alice).await;
        let priya_ocean = seed_member_graph(&owner, account_id, priya).await;

        // Priya has NO oauth_tokens row at all — only a CONFIRMED domain-
        // directory link, exactly what `match_and_persist_chat_verified_
        // email` writes from a real roster-reconcile pass.
        sqlx::query(
            "INSERT INTO connector_identities \
                 (provider, workspace_id, external_user_id, canonical_name, user_id, \
                  link_method, link_verified_at) \
             VALUES ('google_chat', 'alice@k12isd.example', '1004', 'Priya Raman', $1, \
                     'verified_email', now())",
        )
        .bind(priya)
        .execute(&owner)
        .await
        .unwrap();
        // Alice's own oauth_tokens row must carry the SAME workspace_id
        // (her own connected email) for `account_chat_workspace_ids` to
        // include it — matching `reader_workspace_id`'s real-world anchor.
        sqlx::query("UPDATE oauth_tokens SET provider_workspace_id = 'alice@k12isd.example' WHERE account_id = $1 AND owner_user_id = $2 AND provider = 'google_chat'")
            .bind(account_id)
            .bind(alice)
            .execute(&owner)
            .await
            .unwrap();

        let crate::connectors::slack::FanOut { mut graphs, unmapped_members: unmapped, .. } =
            resolve_chat_fanout_destinations(&app, &app, account_id).await.unwrap();
        graphs.sort();
        let mut expected = vec![alice_ocean, priya_ocean];
        expected.sort();
        assert_eq!(
            graphs, expected,
            "a member resolved ONLY via the domain-directory verified link (no oauth_tokens row \
             at all) must still be a fan-out destination — matching by sub alone would silently \
             exclude exactly the person this feature exists to reach"
        );
        assert_eq!(unmapped, 0, "both members are mapped — one by sub, one by verified email");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn ingest_space_items_refuses_to_write_without_a_dek_and_never_half_writes_pg() {
        // **Conformance test 2 (tenant-resolution refusal), design doc §6**,
        // in the form this write path can actually express it: an account
        // whose DEK cannot be resolved writes NOTHING — a null-key
        // "encryption" would be a security hole, so the write bails rather
        // than falling through to a placeholder (`slack_events`' own
        // posture). Every item is reported failed, so the caller's
        // commit-after-ingest check leaves the cursor untouched.
        let Some(base) = base_url() else {
            eprintln!("skipping ingest_space_items_refuses_to_write_without_a_dek_and_never_half_writes_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_ingest_nodek";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let user_id = seed_user(&owner).await;
        let account_id = seed_account(&owner, user_id).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        let item = NormalizedItem {
            source_type: SOURCE_TYPE.to_string(),
            source_id: "spaces/AAA/messages/m1".to_string(),
            content: "hello".to_string(),
            participants: vec!["Alice Ng".to_string()],
            timestamp: Utc::now(),
            scope: None,
            thread_id: None,
            channel_id: Some("spaces/AAA".to_string()),
            author_id: Some("1001".to_string()),
            author_name: Some("Alice Ng".to_string()),
        };
        let state = crate::test_app_state();
        seed_space_route(&owner, account_id, "spaces/AAA", Some(graph_id)).await;
        let (grant, _) = crate::ingest::OrgSharedGrant::google_chat_space(
            &app, &app, &crate::tenancy::TenantId::Consumer, account_id, crate::routes::slack::AccountPlane::Personal, "spaces/AAA", None,
        )
        .await
        .unwrap()
        .expect("routed");
        let outcome = ingest_space_items(
            &state,
            &crate::tenancy::TenantId::Consumer,
            &app,
            &grant,
            "eng-platform",
            Some(user_id),
            std::slice::from_ref(&item),
        )
        .await;
        assert!(!outcome.fully_succeeded());
        assert_eq!(outcome.items_failed, 1);
        assert_eq!(outcome.atoms_written, 0);
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
            .bind(graph_id)
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(n, 0, "an unresolvable DEK must leave NOTHING behind");

        // No destinations at all (a fan-out that resolved to zero graphs) is
        // a quiet no-op that also writes nothing and reports no success —
        // §1.10 lesson 4: an infrastructure gap never writes a terminal row.
        let nowhere = crate::ingest::OrgSharedGrant::google_chat_fanout_for_test(account_id, "spaces/AAA", Vec::new());
        let outcome = ingest_space_items(
            &state,
            &crate::tenancy::TenantId::Consumer,
            &app,
            &nowhere,
            "eng-platform",
            Some(user_id),
            std::slice::from_ref(&item),
        )
        .await;
        assert_eq!(outcome.atoms_written, 0);
        assert_eq!(outcome.destinations_ok, 0);

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn ingest_space_items_writes_chunk_suffixed_atoms_to_every_destination_and_is_idempotent_pg() {
        // The Path-1 write itself (§3.9/D10), end to end on the dev-mode
        // inline path (`state.r2 == None`), with a REAL wrapped DEK.
        //
        // Path 1, not Path 2, deliberately: a space's destination may be a
        // TEAM ocean, which `ingest::is_own_writable_ocean` refuses
        // by design. On the personal plane the destination is
        // `kind='personal'` and WOULD pass that gate — which is precisely
        // why design §3.8/line 524 forbids shipping the Path-2 shape here.
        let Some(base) = base_url() else {
            eprintln!("skipping ingest_space_items_writes_chunk_suffixed_atoms_to_every_destination_and_is_idempotent_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        // SAFETY: single-threaded test harness (`--test-threads=1`) plus the
        // crate-wide `MASTER_KEK_HEX_ENV_LOCK` held for this whole body —
        // the same convention every other `MASTER_KEK_HEX`-touching test in
        // this tree uses.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", "1".repeat(64));
        }

        let db = "gws_g44_path1_write";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let user_id = seed_user(&owner).await;
        let account_id = seed_account(&owner, user_id).await;
        // A fan-out lands in member oceans only, and the door re-checks that
        // each destination still is one.
        let colleague = seed_user(&owner).await;
        let graph_a = seed_member_graph(&owner, account_id, colleague).await;
        let graph_b = seed_member_graph(&owner, account_id, user_id).await;

        let kek = coralswarm_core::kek::load_kek().expect("MASTER_KEK_HEX set above");
        let dek = coralswarm_core::kek::generate_dek();
        let wrapped = coralswarm_core::kek::wrap_dek(&kek, &dek).unwrap();
        sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
            .bind(&wrapped)
            .bind(account_id)
            .execute(&owner)
            .await
            .unwrap();

        let item = NormalizedItem {
            source_type: SOURCE_TYPE.to_string(),
            source_id: "spaces/AAA/messages/m1".to_string(),
            content: "morning all, deploy window opens at 10am".to_string(),
            participants: vec!["Alice Ng".to_string()],
            timestamp: Utc::now(),
            scope: None,
            thread_id: Some("spaces/AAA/threads/t1".to_string()),
            channel_id: Some("spaces/AAA".to_string()),
            author_id: Some("1001".to_string()),
            author_name: Some("Alice Ng".to_string()),
        };
        let state = crate::test_app_state();
        seed_space_route(&owner, account_id, "spaces/AAA", None).await;
        let grant = crate::ingest::OrgSharedGrant::google_chat_fanout_for_test(account_id, "spaces/AAA", vec![graph_a, graph_b]);
        // Captured originals are retained but managed-blocked without an execution
        // owner. Poison the actual early source write, not an extraction job that
        // is correctly never minted. Keep the reachable late keyword-job fault.
        for (table, predicate) in [
            ("source_versions", format!("NEW.graph_id IN ('{graph_a}'::uuid,'{graph_b}'::uuid)")),
            ("jobs", format!("NEW.kind='EmbedAtoms' AND NEW.account_id='{account_id}'::uuid")),
        ] {
            sqlx::query(&format!("CREATE OR REPLACE FUNCTION fail_source_job() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF {predicate} THEN RAISE EXCEPTION 'injected source write failure'; END IF; RETURN NEW; END $$"))
                .execute(&owner).await.unwrap();
            sqlx::query(&format!("CREATE TRIGGER fail_source_job BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION fail_source_job()"))
                .execute(&owner).await.unwrap();
        let outcome = ingest_space_items(
            &state,
            &crate::tenancy::TenantId::Consumer,
            &app,
            &grant,
            "eng-platform",
            Some(user_id),
            std::slice::from_ref(&item),
        )
        .await;
            assert!(!outcome.fully_succeeded());
            let rows:i64=sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=ANY($1)")
                .bind(vec![graph_a,graph_b]).fetch_one(&owner).await.unwrap();
            assert_eq!(rows,0);
            let sources:i64=sqlx::query_scalar("SELECT count(*) FROM source_items WHERE graph_id=ANY($1)")
                .bind(vec![graph_a,graph_b]).fetch_one(&owner).await.unwrap();
            assert_eq!(sources,0);
            assert_eq!(
                (outcome.atoms_written, outcome.destinations_ok, outcome.destinations_failed, outcome.items_failed),
                (0, 0, 2, 2),
                "each independently attempted destination must report its injected failure",
            );
            let partials: (i64, i64, i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM source_versions WHERE graph_id=ANY($1)), \
                 (SELECT count(*) FROM source_version_chunks WHERE graph_id=ANY($1)), \
                 (SELECT count(*) FROM corpus_capture_admissions WHERE graph_id=ANY($1)), \
                 (SELECT count(*) FROM jobs WHERE account_id=$2 AND kind='EmbedAtoms'), \
                 (SELECT count(*) FROM jobs WHERE account_id=$2 AND kind='ExtractSourceAtoms')",
            ).bind(vec![graph_a, graph_b]).bind(account_id).fetch_one(&owner).await.unwrap();
            assert_eq!(partials, (0, 0, 0, 0, 0), "capture/chunks/admission/jobs must roll back together");
            sqlx::query(&format!("DROP TRIGGER fail_source_job ON {table}")).execute(&owner).await.unwrap();
        }
        sqlx::query("DROP FUNCTION fail_source_job()").execute(&owner).await.unwrap();
        let outcome = ingest_space_items(
            &state,
            &crate::tenancy::TenantId::Consumer,
            &app,
            &grant,
            "eng-platform",
            Some(user_id),
            std::slice::from_ref(&item),
        )
        .await;
        assert!(outcome.fully_succeeded(), "{outcome:?}");
        assert_eq!(outcome.destinations_ok, 2, "both destinations succeeded independently");
        assert_eq!(outcome.atoms_written, 2, "one chunk each, in two oceans");

        // The atom shape: CHUNK-SUFFIXED source_id (what `routes::ingest`
        // and `routes::slack_events` also write), structured provenance
        // stamped, and the context header prepended to the stored body.
        let rows: Vec<(Uuid, String, String, Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT graph_id, source_id, source_type, channel_id, author_id, content_inline \
             FROM atoms WHERE graph_id = ANY($1) ORDER BY graph_id",
        )
        .bind(vec![graph_a, graph_b])
        .fetch_all(&owner)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        for (_, source_id, source_type, channel_id, author_id, inline) in &rows {
            assert_eq!(source_id, "spaces/AAA/messages/m1:chunk:0");
            assert_eq!(source_type, "google_chat");
            assert_eq!(channel_id.as_deref(), Some("spaces/AAA"));
            assert_eq!(author_id.as_deref(), Some("1001"));
            let inline = inline.as_deref().expect("dev-mode inline content");
            assert!(inline.starts_with("Alice Ng in eng-platform · "), "{inline}");
            assert!(inline.contains("deploy window opens at 10am"));
        }

        // Idempotent: a redelivered push, a re-walked catch-up range, or a
        // second member's subscription delivering the same message all
        // converge on `ON CONFLICT DO NOTHING`.
        let again = ingest_space_items(
            &state,
            &crate::tenancy::TenantId::Consumer,
            &app,
            &grant,
            "eng-platform",
            Some(user_id),
            std::slice::from_ref(&item),
        )
        .await;
        assert!(again.fully_succeeded());
        assert_eq!(again.atoms_written, 0, "a duplicate delivery writes nothing new");
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = ANY($1)")
            .bind(vec![graph_a, graph_b])
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(n, 2);

        let versions:i64=sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=ANY($1)")
            .bind(vec![graph_a,graph_b]).fetch_one(&owner).await.unwrap();
        assert_eq!(versions,2,"each destination has one retry-stable source snapshot");
        let snapshots:Vec<(String,String)>=sqlx::query_as(
            "SELECT provider_namespace,raw_inline FROM source_versions WHERE graph_id=ANY($1)",
        ).bind(vec![graph_a,graph_b]).fetch_all(&owner).await.unwrap();
        assert!(snapshots.iter().all(|(namespace,text)|namespace=="google_chat:space:spaces/AAA" && text==&item.content));

        let admissions: Vec<(Uuid, String, String, Option<String>, Option<Uuid>, Option<Uuid>, Option<Uuid>, Uuid)> = sqlx::query_as(
            "SELECT v.graph_id,v.extraction_status,a.state,a.reason,a.run_id,a.unit_id,a.captured_by,a.account_id \
             FROM source_versions v JOIN corpus_capture_admissions a \
             ON a.source_version_id=v.version_id AND a.graph_id=v.graph_id \
             WHERE v.graph_id=ANY($1) ORDER BY v.graph_id",
        ).bind(vec![graph_a, graph_b]).fetch_all(&owner).await.unwrap();
        assert_eq!(admissions.len(), 2, "each destination retains its actual admission receipt");
        let mut expected_graphs = vec![graph_a, graph_b];
        expected_graphs.sort();
        assert_eq!(admissions.iter().map(|r| r.0).collect::<Vec<_>>(), expected_graphs);
        for (_, extraction, state, reason, run, unit, captured_by, account) in admissions {
            assert_eq!((extraction.as_str(), state.as_str(), reason.as_deref()),
                ("blocked", "blocked", Some("managed_admission_required")));
            assert_eq!((run, unit, captured_by, account), (None, None, Some(user_id), account_id),
                "capture must not fabricate a managed owner, budget or semantic completion");
        }
        let work: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM source_version_chunks WHERE graph_id=ANY($1)), \
             (SELECT count(*) FROM jobs WHERE account_id=$2 AND kind='EmbedAtoms' AND status='pending'), \
             (SELECT count(*) FROM jobs WHERE account_id=$2 AND kind='ExtractSourceAtoms')",
        ).bind(vec![graph_a, graph_b]).bind(account_id).fetch_one(&owner).await.unwrap();
        assert_eq!(work, (2, 2, 0), "retry-stable chunks/keyword jobs do not admit managed extraction");

        // And C9 reaches those REAL atoms — the whole point of resolving
        // concrete chunk ids (§3.6 deviation, see `propagate_deletions`).
        propagate_deletions(&app, account_id, graph_a, &[item.source_id.clone()])
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = ANY($1)")
            .bind(vec![graph_a, graph_b])
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(
            n, 0,
            "an upstream delete removes EVERY copy across the account (the (source_type, \
             source_id) pair IS the coral's identity across oceans)"
        );

        let sources:i64=sqlx::query_scalar("SELECT count(*) FROM source_items WHERE graph_id=ANY($1)")
            .bind(vec![graph_a,graph_b]).fetch_one(&owner).await.unwrap();
        assert_eq!(sources,0,"provider delete retracts full snapshots alongside retrieval chunks");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
        // SAFETY: see the set_var above — same lock, same single-threaded
        // harness.
        unsafe {
            std::env::remove_var("MASTER_KEK_HEX");
        }
    }

    // ── Review round 1 + security addendum ───────────────────────────────

    #[test]
    fn message_fetch_permanence_classification() {
        use reqwest::StatusCode;
        // Permanent: the message will never be readable again with this
        // token, so retrying forever is what wedges a space for 28 days
        // (review round 1, H2).
        for permanent in [StatusCode::NOT_FOUND, StatusCode::FORBIDDEN, StatusCode::GONE] {
            assert!(message_fetch_is_permanent(permanent), "{permanent} must be consumed");
        }
        // Transient: MUST block the watermark, or a rate-limited burst
        // silently drops messages.
        for transient in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
            StatusCode::UNAUTHORIZED,
        ] {
            assert!(
                !message_fetch_is_permanent(transient),
                "{transient} must NOT be treated as permanent — it would silently drop a message"
            );
        }
    }

    #[test]
    fn a_seeded_watermark_is_preserved_rather_than_floored_to_28_days() {
        // Review round 1, H1's second half: the backfill→catch-up handoff.
        // With `sync_token` NULL (the pre-fix state at handoff),
        // `floored_watermark` returns `now - 28d` and the space re-walks a
        // full 28 days of events the backfill just ingested.
        let now = DateTime::parse_from_rfc3339("2026-08-30T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let floor = (now - chrono::Duration::days(28)).to_rfc3339();
        assert_eq!(floored_watermark(None, now), floor, "the pre-fix handoff state");
        // With the seed, the very next catch-up starts where the backfill
        // stopped.
        let seeded = "2026-08-29T12:00:00+00:00";
        assert_eq!(floored_watermark(Some(seeded), now), seeded);
    }

    #[tokio::test]
    async fn an_offboarded_ex_member_is_never_elected_as_a_space_reader_pg() {
        // **Security addendum, H1 (reproduced).** `clerk_provision::
        // remove_org_membership` deletes the `account_members` row and never
        // touches `oauth_tokens`, so a departed member's Chat token outlives
        // their membership. Before the `EXISTS(account_members)` gate, the
        // election ordered by `updated_at DESC` on `oauth_tokens` ALONE — so
        // the most-recently-refreshed token won even if its owner had been
        // offboarded, and the sweep would then refresh + RE-PERSIST that
        // token, read every routed space with it, and re-create a Workspace
        // Events subscription in the ex-member's name.
        let Some(base) = base_url() else {
            eprintln!("skipping an_offboarded_ex_member_is_never_elected_as_a_space_reader_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_offboard_reader";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let departed = seed_user(&owner).await;
        let current = seed_user(&owner).await;
        let account_id = seed_account(&owner, current).await;

        // BOTH have live google_chat tokens. Only `current` is a member.
        // `departed`'s token is the MORE RECENTLY updated, so it wins the
        // `updated_at DESC` ordering — which is exactly what made this
        // exploitable rather than theoretical.
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, owner_user_id, provider_user_id, access_token, refresh_token, updated_at) \
             VALUES ($1, 'google_chat', 'test-scope', $2, 'sub-current', 'enc', 'enc', now() - interval '1 hour')",
        )
        .bind(account_id)
        .bind(current)
        .execute(&owner)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, owner_user_id, provider_user_id, access_token, refresh_token, updated_at) \
             VALUES ($1, 'google_chat', 'test-scope', $2, 'sub-departed', 'enc', 'enc', now())",
        )
        .bind(account_id)
        .bind(departed)
        .execute(&owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(account_id)
            .bind(current)
            .execute(&owner)
            .await
            .unwrap();

        assert_eq!(
            elect_space_reader(&app, account_id, &[]).await.unwrap(),
            Some(current),
            "the election must skip the offboarded member even though their token sorts FIRST on \
             updated_at DESC — an ex-member's surviving credential is not a candidate"
        );

        // And once the last MEMBER's connection goes, there is nobody —
        // never a silent fallback onto the ex-member's live token.
        sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(current)
            .execute(&owner)
            .await
            .unwrap();
        assert_eq!(
            elect_space_reader(&app, account_id, &[]).await.unwrap(),
            None,
            "\"nobody left\" must never degrade into \"use the departed member's token\""
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn reader_failover_walks_past_two_broken_candidates_to_a_working_third_pg() {
        // **Security addendum, L2.** The election orders by `updated_at
        // DESC`, and `store_token_bundle` only runs on SUCCESS — so a
        // failing token's sort position never moves. A failover that could
        // exclude only ONE prior candidate therefore re-picked the same two
        // most-recent-but-broken members every tick while logging
        // `connector_health = true` "nobody left", with a working third
        // member sitting right behind them. The fix is an exclude LIST.
        let Some(base) = base_url() else {
            eprintln!("skipping reader_failover_walks_past_two_broken_candidates_to_a_working_third_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_failover_list";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let a = seed_user(&owner).await;
        let b = seed_user(&owner).await;
        let c = seed_user(&owner).await;
        let account_id = seed_account(&owner, a).await;
        for (i, user_id) in [a, b, c].iter().enumerate() {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account_id)
                .bind(user_id)
                .execute(&owner)
                .await
                .unwrap();
            // a newest, then b, then c — so a and b are always picked first.
            sqlx::query(
                "INSERT INTO oauth_tokens (account_id, provider, scope, owner_user_id, provider_user_id, access_token, refresh_token, updated_at) \
                 VALUES ($1, 'google_chat', 'test-scope', $2, $3, 'enc', 'enc', now() - ($4 || ' minutes')::interval)",
            )
            .bind(account_id)
            .bind(user_id)
            .bind(format!("sub-{i}"))
            .bind(i.to_string())
            .execute(&owner)
            .await
            .unwrap();
        }

        // The failover walk, exactly as `reader_credentials` performs it.
        let first = elect_space_reader(&app, account_id, &[]).await.unwrap().unwrap();
        assert_eq!(first, a, "newest token first");
        let second = elect_space_reader(&app, account_id, &[first]).await.unwrap().unwrap();
        assert_eq!(second, b);
        let third = elect_space_reader(&app, account_id, &[first, second]).await.unwrap().unwrap();
        assert_eq!(
            third, c,
            "a two-candidate exclusion must reach the THIRD member — the single-exclusion version \
             livelocked between the first two forever"
        );
        assert_eq!(
            elect_space_reader(&app, account_id, &[a, b, c]).await.unwrap(),
            None,
            "only once EVERY candidate is exhausted is the answer \"nobody\""
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn revocation_still_works_after_the_app_is_removed_from_the_space_pg() {
        // **Security addendum, M1 (reproduced).** `record_removed_from_space`
        // sets `active = false`, but the per-member subscription is on
        // `spaces/-` and `resolve_chat_space_account` ignores `active`, so a
        // `membership.v1.deleted` still ARRIVES afterwards. Sharing one
        // `active`-gated resolver between grant and revoke meant that
        // departure resolved no team, the handler swallowed it, and the
        // member kept a seat on the destination TEAM ocean — with
        // `history_from = NULL`, i.e. unbounded history including everything
        // predating the space.
        let Some(base) = base_url() else {
            eprintln!("skipping revocation_still_works_after_the_app_is_removed_from_the_space_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_revoke_inactive";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;
        let team_id = seed_team(&owner, account_id, graph_id, alice).await;

        record_added_to_space(&app, &app, AccountPlane::Personal, account_id, None, &test_space("spaces/RM"))
            .await
            .unwrap();
        pick_space_destination(
            &app,
            &app,
            AccountPlane::Personal,
            account_id,
            "spaces/RM",
            PickedDestination::Single(graph_id),
        )
        .await
        .unwrap();
        let source = crate::seats::google_chat_space_source("spaces/RM");
        crate::routes::slack_interactivity::seat_members_for_source(&app, account_id, team_id, &[alice], &source, None)
            .await;

        // While ACTIVE, both resolvers agree.
        assert_eq!(
            resolve_space_team_for_grant(&app, account_id, "spaces/RM").await.unwrap(),
            Some(team_id)
        );
        assert_eq!(
            resolve_space_team_for_revoke(&app, account_id, "spaces/RM").await.unwrap(),
            Some(team_id)
        );

        // The app is removed from the space.
        assert!(record_removed_from_space(&app, account_id, "spaces/RM").await.unwrap());

        // GRANT must now refuse — never seat people into a space we left.
        assert_eq!(
            resolve_space_team_for_grant(&app, account_id, "spaces/RM").await.unwrap(),
            None,
            "the grant side MUST keep its active predicate"
        );
        // REVOKE must still resolve — always let go.
        assert_eq!(
            resolve_space_team_for_revoke(&app, account_id, "spaces/RM").await.unwrap(),
            Some(team_id),
            "the revoke side must NOT require active, or a departure after removal leaves an \
             unbounded-history seat standing forever"
        );

        // And the departure genuinely lands.
        let outcome = revoke_space_seat(&app, account_id, team_id, alice, "spaces/RM").await.unwrap();
        assert_eq!(outcome, crate::seats::RevokeOutcome::SeatRevoked);
        let seats: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM team_members WHERE team_id = $1 AND user_id = $2 AND departing_at IS NULL",
        )
        .bind(team_id)
        .bind(alice)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(seats, 0);

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn roster_reconcile_diffs_outstanding_grants_and_revokes_the_departed_pg() {
        // **Security addendum, M2.** The reconcile used to be grant-only,
        // which left departure-driven revocation with no backstop: a
        // `membership.v1.deleted` lost to a 7-day subscription lapse, a
        // deploy window, or the 28-day `spaceEvents` retention floor was
        // unrecoverable, because `space_events_filter` names only
        // message events. This drives the exact sequence
        // `reconcile_space_roster`'s revoke half runs — outstanding grants
        // MINUS the members still present — without needing a live
        // `spaces.members.list`.
        let Some(base) = base_url() else {
            eprintln!("skipping roster_reconcile_diffs_outstanding_grants_and_revokes_the_departed_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_reconcile_revoke";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let stayed = seed_user(&owner).await;
        let left = seed_user(&owner).await;
        let account_id = seed_account(&owner, stayed).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;
        let team_id = seed_team(&owner, account_id, graph_id, stayed).await;
        let source = crate::seats::google_chat_space_source("spaces/RC");

        crate::routes::slack_interactivity::seat_members_for_source(
            &app,
            account_id,
            team_id,
            &[stayed, left],
            &source,
            None,
        )
        .await;

        let granted = crate::seats::users_granted_by_source(&app, account_id, team_id, &source)
            .await
            .unwrap();
        assert_eq!(granted.len(), 2, "both were seated by this space");

        // The live roster now contains only `stayed` — the diff the
        // reconcile computes from `SeatingOutcome::matched_user_ids`.
        let still_present: HashSet<Uuid> = [stayed].into_iter().collect();
        let mut revoked = 0usize;
        for user_id in granted {
            if still_present.contains(&user_id) {
                continue;
            }
            revoke_space_seat(&app, account_id, team_id, user_id, "spaces/RC").await.unwrap();
            revoked += 1;
        }
        assert_eq!(revoked, 1, "exactly the departed member is revoked");

        let seated: Vec<Uuid> = sqlx::query_scalar(
            "SELECT user_id FROM team_members WHERE team_id = $1 AND departing_at IS NULL",
        )
        .bind(team_id)
        .fetch_all(&owner)
        .await
        .unwrap();
        assert_eq!(seated, vec![stayed], "the member who stayed keeps their seat");
        let left_grants: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM team_seat_grants WHERE team_id = $1 AND user_id = $2",
        )
        .bind(team_id)
        .bind(left)
        .fetch_one(&owner)
        .await
        .unwrap();
        assert_eq!(left_grants, 0, "and the departed member's grant is gone");

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn the_seating_identity_query_never_returns_a_credential_and_never_crosses_accounts_pg() {
        // Design doc risk 10: `resolve_google_identity_in_account` reads
        // `oauth_tokens` with a WIDENED provider predicate
        // (`LIKE 'google%'`). It returns ids and must never return a secret,
        // and its account scope is the one thing a careless later edit would
        // drop. Both pinned here, on the function G4.4's seating actually
        // calls (G4.3 pins the same invariants for the SECURITY DEFINER
        // bootstrap resolver).
        let Some(base) = base_url() else {
            eprintln!("skipping the_seating_identity_query_never_returns_a_credential_and_never_crosses_accounts_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_identity_guard";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let account_a = seed_account(&owner, alice).await;
        let outsider = seed_user(&owner).await;
        let account_b = seed_account(&owner, outsider).await;
        seed_oauth_token(&owner, account_a, alice, "google_chat", "shared-sub").await;

        assert_eq!(
            resolve_google_identity_in_account(&app, account_a, "shared-sub").await.unwrap(),
            Some(alice)
        );
        assert_eq!(
            resolve_google_identity_in_account(&app, account_b, "shared-sub").await.unwrap(),
            None,
            "the account scope is the whole safety property — one careless edit dropping it \
             would seat a stranger"
        );
        // §3.3's widened predicate: a colleague who connected Gmail but not
        // yet Chat is still identifiable, which is the point.
        let bob = seed_user(&owner).await;
        seed_oauth_token(&owner, account_a, bob, "google_gmail", "gmail-only-sub").await;
        assert_eq!(
            resolve_google_identity_in_account(&app, account_a, "gmail-only-sub").await.unwrap(),
            Some(bob),
            "refusing to identify a Gmail-connected colleague would be I4 read backwards"
        );

        // Identification only — the source text of the query this module
        // uses selects `owner_user_id` and nothing else. Guarded here
        // because the hazard is a future \"just reuse this to get the
        // token\" edit, not today's code.
        let src = include_str!("google_chat.rs");
        let query_start = src
            .find("SELECT owner_user_id FROM oauth_tokens")
            .expect("the account-scoped identity query must exist verbatim");
        let query = &src[query_start..query_start + 400];
        for forbidden in ["access_token", "refresh_token", "user_token"] {
            assert!(
                !query.contains(forbidden),
                "resolve_google_identity_in_account must never select a credential column \
                 ({forbidden} found) — it identifies, it never loads a token"
            );
        }

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    // ── G4.5 — the mailbox archetype (`GROUP_CHAT`/`DIRECT_MESSAGE`) ────────

    fn test_mailbox_space(name: &str, space_type: &str) -> ChatSpace {
        ChatSpace {
            name: name.to_string(),
            space_type: space_type.to_string(),
            display_name: "Alice & Bob".to_string(),
            space_threading_state: "UNTHREADED_MESSAGES".to_string(),
            history_state: "HISTORY_ON".to_string(),
            single_user_bot_dm: false,
            membership_count: None,
        }
    }

    #[tokio::test]
    async fn mailbox_optin_creates_row_and_reopting_in_reactivates_without_moving_the_destination_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping mailbox_optin_creates_row_and_reopting_in_reactivates_without_moving_the_destination_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g45_optin_roundtrip";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        let space = test_mailbox_space("spaces/DM1", "DIRECT_MESSAGE");
        let row = record_mailbox_optin(&app, account_id, alice, &space, graph_id).await.unwrap();
        assert_eq!(row.space_id, "spaces/DM1");
        assert_eq!(row.space_type, "DIRECT_MESSAGE");
        assert_eq!(row.graph_id, graph_id);
        assert!(row.active);

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/DM1'")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(count, 1, "one opt-in must create exactly one row");

        // Opt out, then back in: reactivates the SAME row (never a second
        // one) and never moves the destination — mirrors
        // `record_added_to_space`'s "never re-point" posture for the
        // container archetype, even though in practice the destination is
        // always this same owner's one private ocean.
        assert!(opt_out_of_mailbox_space(&app, account_id, alice, "spaces/DM1").await.unwrap());
        let after_optout: (bool,) = sqlx::query_as("SELECT active FROM google_chat_watched_spaces WHERE space_id = 'spaces/DM1'")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert!(!after_optout.0);

        let reopt = record_mailbox_optin(&app, account_id, alice, &space, graph_id).await.unwrap();
        assert!(reopt.active, "re-opting in must reactivate");
        assert_eq!(reopt.graph_id, graph_id, "the destination must never move on a re-opt-in");
        let count_after: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/DM1'")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(count_after, 1, "a re-opt-in must reactivate the SAME row, never insert a second one");

        // Opting OUT of a space nobody ever opted into is a clean `false`,
        // never an error.
        assert!(!opt_out_of_mailbox_space(&app, account_id, alice, "spaces/never-opted-in").await.unwrap());

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn mailbox_optin_refuses_a_space_archetype_and_writes_nothing_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping mailbox_optin_refuses_a_space_archetype_and_writes_nothing_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g45_optin_wrong_archetype";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        // A `SPACE` — the container archetype's own picker is the correct
        // route, not this one (`routes::google_chat::opt_in_to_mailbox_space`
        // re-reads `spaces.get` before ever reaching this function, for
        // exactly this reason: never trust a caller-supplied archetype).
        let space = test_space("spaces/SHARED");
        let err = record_mailbox_optin(&app, account_id, alice, &space, graph_id).await.unwrap_err();
        assert!(matches!(err, MailboxOptInError::WrongArchetype));

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/SHARED'")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(count, 0, "a refused opt-in must write NOTHING — not even a partial row");

        // An UNRECOGNISED archetype value (empty string, `SPACE_TYPE_
        // UNSPECIFIED`, or any future Chat API addition) must be refused the
        // SAME clean, typed way — allowlisting `GROUP_CHAT`/`DIRECT_MESSAGE`
        // (CodeRabbit round 1, PR #811) rather than denylisting `SPACE`
        // alone is what makes this a `WrongArchetype` refusal instead of an
        // opaque `MailboxOptInError::Db` surfaced only once the archetype
        // CHECK constraint (0198) rejects the write downstream.
        let mut unspecified = test_space("spaces/UNSPECIFIED-KIND");
        unspecified.space_type = "SPACE_TYPE_UNSPECIFIED".to_string();
        let err = record_mailbox_optin(&app, account_id, alice, &unspecified, graph_id).await.unwrap_err();
        assert!(
            matches!(err, MailboxOptInError::WrongArchetype),
            "an unrecognised space_type must be refused as WrongArchetype, not surface as a raw \
             DB error — got {err:?}"
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/UNSPECIFIED-KIND'")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(count, 0);

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn two_owners_can_independently_optin_the_same_physical_space_pg() {
        // The whole point of the disjoint partial unique index
        // (`uq_google_chat_watched_spaces_mailbox`, migration 0198's own
        // header): two colleagues in the SAME account may each opt the SAME
        // group chat into their OWN private ocean, independently.
        let Some(base) = base_url() else {
            eprintln!("skipping two_owners_can_independently_optin_the_same_physical_space_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g45_two_owners_one_space";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let bob = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member'), ($1, $3, 'member')")
            .bind(account_id)
            .bind(alice)
            .bind(bob)
            .execute(&owner)
            .await
            .unwrap();
        let graph_alice = seed_primary_graph(&owner, account_id).await;
        let graph_bob = seed_member_graph(&owner, account_id, bob).await;

        let space = test_mailbox_space("spaces/GROUP1", "GROUP_CHAT");
        record_mailbox_optin(&app, account_id, alice, &space, graph_alice).await.unwrap();
        record_mailbox_optin(&app, account_id, bob, &space, graph_bob).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/GROUP1'")
            .fetch_one(&owner)
            .await
            .unwrap();
        assert_eq!(count, 2, "each owner's opt-in is its own row");

        // Owner-scoped listing: alice's view of the space names ONLY her own
        // destination, never bob's.
        let alice_view = list_watched_mailbox_spaces_for_owner(&app, account_id, alice).await.unwrap();
        assert_eq!(alice_view.len(), 1);
        assert_eq!(alice_view["spaces/GROUP1"].graph_id, graph_alice);
        let bob_view = list_watched_mailbox_spaces_for_owner(&app, account_id, bob).await.unwrap();
        assert_eq!(bob_view.len(), 1);
        assert_eq!(bob_view["spaces/GROUP1"].graph_id, graph_bob);

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn resolve_chat_mailbox_space_owners_returns_every_account_owner_pair_and_never_fails_closed_pg() {
        // Mirrors `resolve_chat_space_accounts_returns_every_account_holding_a_
        // container_row_pg`'s own shape/rationale for the mailbox archetype:
        // deliberately NOT fail-closed on more than one match — two DIFFERENT
        // accounts, each with their own opting-in member, must BOTH be found.
        let Some(base) = base_url() else {
            eprintln!("skipping resolve_chat_mailbox_space_owners_returns_every_account_owner_pair_and_never_fails_closed_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g45_resolve_mailbox_owners";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        assert!(
            resolve_chat_mailbox_space_owners(&owner, "spaces/UNKNOWN").await.unwrap().is_empty(),
            "a space nobody has opted into resolves to nothing, not an error"
        );
        assert!(resolve_chat_mailbox_space_owners(&owner, "").await.unwrap().is_empty());

        let alice = seed_user(&owner).await;
        let account_a = seed_account(&owner, alice).await;
        let graph_a = seed_primary_graph(&owner, account_a).await;
        let carol = seed_user(&owner).await;
        let account_c = seed_account(&owner, carol).await;
        let graph_c = seed_primary_graph(&owner, account_c).await;

        let space = test_mailbox_space("spaces/CROSS-ACCOUNT-DM", "DIRECT_MESSAGE");
        record_mailbox_optin(&app, account_a, alice, &space, graph_a).await.unwrap();
        record_mailbox_optin(&app, account_c, carol, &space, graph_c).await.unwrap();

        let mut pairs = resolve_chat_mailbox_space_owners(&owner, "spaces/CROSS-ACCOUNT-DM").await.unwrap();
        pairs.sort();
        let mut expected = vec![(account_a, alice), (account_c, carol)];
        expected.sort();
        assert_eq!(
            pairs, expected,
            "both independently-opted-in (account, owner) pairs must be returned — a push for \
             this physical space must reach every one of them, never just one"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn mailbox_cursor_functions_are_owner_scoped_and_never_cross_contaminate_pg() {
        // The SAME `fetch_pending`/`commit_pending`/`load_space_sync_state`
        // funnel G4.4 built for the container archetype is reused here
        // (§9's "reuse the same ingest code" instruction) — this test proves
        // the `owner_user_id` parameter this PR added to thread it actually
        // isolates two owners on the SAME space_id, not merely that it
        // compiles.
        let Some(base) = base_url() else {
            eprintln!("skipping mailbox_cursor_functions_are_owner_scoped_and_never_cross_contaminate_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g45_mailbox_cursor_isolation";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let bob = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member'), ($1, $3, 'member')")
            .bind(account_id)
            .bind(alice)
            .bind(bob)
            .execute(&owner)
            .await
            .unwrap();
        let graph_alice = seed_primary_graph(&owner, account_id).await;
        let graph_bob = seed_member_graph(&owner, account_id, bob).await;

        let space = test_mailbox_space("spaces/SHARED-GROUP", "GROUP_CHAT");
        record_mailbox_optin(&app, account_id, alice, &space, graph_alice).await.unwrap();
        record_mailbox_optin(&app, account_id, bob, &space, graph_bob).await.unwrap();

        // Fresh rows read as "nothing pending" for BOTH owners independently.
        let alice_state = load_space_sync_state(&app, account_id, "spaces/SHARED-GROUP", Some(alice))
            .await
            .unwrap()
            .unwrap();
        assert!(alice_state.backfill_complete);
        let bob_state = load_space_sync_state(&app, account_id, "spaces/SHARED-GROUP", Some(bob))
            .await
            .unwrap()
            .unwrap();
        assert!(bob_state.backfill_complete);

        // Arm and advance ALICE's backfill only.
        assert!(arm_backfill_for_space(&app, account_id, "spaces/SHARED-GROUP", Some(alice)).await.unwrap());
        commit_pending(
            &app,
            account_id,
            "spaces/SHARED-GROUP",
            Some(alice),
            PendingCommit::Backfill {
                resume_cursor: Some("alice-page-2".into()),
                complete: false,
                seed_watermark: None,
            },
        )
        .await
        .unwrap();

        let alice_after = load_space_sync_state(&app, account_id, "spaces/SHARED-GROUP", Some(alice))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(alice_after.backfill_cursor.as_deref(), Some("alice-page-2"));
        assert!(!alice_after.backfill_complete);

        // Bob's row must be COMPLETELY untouched by Alice's pass — no shared
        // state, no accidental `owner_user_id IS NULL` fallthrough.
        let bob_untouched = load_space_sync_state(&app, account_id, "spaces/SHARED-GROUP", Some(bob))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bob_untouched, bob_state, "bob's cursor must be byte-identical to before alice's pass");
        // Bob's row is still fresh (never armed — `backfill_complete` reads
        // TRUE, `last_ingested_at IS NULL`), so arming it independently of
        // Alice's already-armed-and-in-progress row must succeed outright.
        // (CodeRabbit round 1, PR #811: the ORIGINAL version of this
        // assertion — `!armed || bob_untouched.backfill_complete` — is a
        // tautology, since `bob_untouched.backfill_complete` is ALWAYS true
        // at this point in the test regardless of what `arm_backfill_for_
        // space` actually returns, so the assertion could never fail no
        // matter how badly owner-scoping was broken.)
        let bob_armed = arm_backfill_for_space(&app, account_id, "spaces/SHARED-GROUP", Some(bob)).await.unwrap();
        assert!(
            bob_armed,
            "arming bob's backfill independently must succeed (it is a distinct, still-fresh row \
             — Alice's own arm/backfill progress must not have consumed or blocked it)"
        );

        // And the container-archetype read (`owner_user_id: None`) must find
        // NOTHING here at all — this space has no `SPACE` row.
        assert!(
            load_space_sync_state(&app, account_id, "spaces/SHARED-GROUP", None).await.unwrap().is_none(),
            "the container-archetype read must never accidentally match a mailbox row"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn due_and_ingestable_mailbox_space_reads_are_owner_scoped_and_exclude_inactive_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping due_and_ingestable_mailbox_space_reads_are_owner_scoped_and_exclude_inactive_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g45_due_mailbox_spaces";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let bob = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member'), ($1, $3, 'member')")
            .bind(account_id)
            .bind(alice)
            .bind(bob)
            .execute(&owner)
            .await
            .unwrap();
        let graph_alice = seed_primary_graph(&owner, account_id).await;
        let graph_bob = seed_member_graph(&owner, account_id, bob).await;

        let dm = test_mailbox_space("spaces/DUE-DM", "DIRECT_MESSAGE");
        let group = test_mailbox_space("spaces/DUE-GROUP", "GROUP_CHAT");
        record_mailbox_optin(&app, account_id, alice, &dm, graph_alice).await.unwrap();
        record_mailbox_optin(&app, account_id, bob, &group, graph_bob).await.unwrap();

        let due = due_mailbox_spaces_for_account(&app, account_id, 10).await.unwrap();
        assert_eq!(due.len(), 2, "both owners' active opt-ins are due");
        assert!(due.iter().any(|(o, s)| *o == alice && s.space_id == "spaces/DUE-DM"));
        assert!(due.iter().any(|(o, s)| *o == bob && s.space_id == "spaces/DUE-GROUP"));

        // The push path's own point read.
        let read_alice = ingestable_mailbox_space(&app, account_id, "spaces/DUE-DM", alice).await.unwrap();
        assert!(read_alice.is_some());
        // Bob has no opt-in on Alice's space.
        let read_bob_on_alice_space = ingestable_mailbox_space(&app, account_id, "spaces/DUE-DM", bob).await.unwrap();
        assert!(read_bob_on_alice_space.is_none(), "an owner's read must never match a DIFFERENT owner's opt-in");

        // Opting out removes it from the due list AND the point read.
        assert!(opt_out_of_mailbox_space(&app, account_id, alice, "spaces/DUE-DM").await.unwrap());
        let due_after = due_mailbox_spaces_for_account(&app, account_id, 10).await.unwrap();
        assert_eq!(due_after.len(), 1, "an opted-out mailbox must stop being due immediately");
        assert!(due_after.iter().all(|(o, _)| *o == bob));
        assert!(
            ingestable_mailbox_space(&app, account_id, "spaces/DUE-DM", alice).await.unwrap().is_none(),
            "an opted-out mailbox must not be ingestable either"
        );

        owner.close().await;
        app.close().await;
        drop_db(&base, db).await;
    }

    #[tokio::test]
    async fn bounded_history_database_continuation_is_member_isolated_pg() {
        let Some(base) = base_url() else { eprintln!("SKIP: ORGT_TEST_BASE_URL required"); return; };
        let db = "chat_bounded_history";
        let _cleanup = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (pool, _, _) = app_role_db(&base, db, false).await;
        let alice = seed_user(&pool).await;
        let bob = seed_user(&pool).await;
        let account = seed_account(&pool, alice).await;
        let graph = seed_primary_graph(&pool, account).await;
        for user in [alice, bob] {
            sqlx::query("INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) VALUES ($1,'spaces/shared','DIRECT_MESSAGE','single',$2,$3,$3)")
                .bind(account).bind(graph).bind(user).execute(&pool).await.unwrap();
        }
        let cursor = HistoryCursor::new(Utc::now(), 30).encode();
        upsert_space_sync_columns(&pool, account, "spaces/shared", Some(alice), None, Some(&cursor), false, false).await.unwrap();
        let a = load_space_sync_state(&pool, account, "spaces/shared", Some(alice)).await.unwrap().unwrap();
        let b = load_space_sync_state(&pool, account, "spaces/shared", Some(bob)).await.unwrap().unwrap();
        assert_eq!(a.backfill_cursor.as_deref(), Some(cursor.as_str()));
        assert!(!a.backfill_complete);
        assert!(b.backfill_complete); assert!(b.backfill_cursor.is_none());
        upsert_space_sync_columns(&pool, account, "spaces/shared", Some(alice), Some("2026-10-30T00:00:00Z"), None, true, true).await.unwrap();
        let done = load_space_sync_state(&pool, account, "spaces/shared", Some(alice)).await.unwrap().unwrap();
        assert!(done.backfill_complete); assert!(done.backfill_cursor.is_none());
        assert_eq!(done.sync_token.as_deref(), Some("2026-10-30T00:00:00Z"));
        sqlx::query("INSERT INTO connector_sources(account_id, owner_user_id, provider, source, enabled, paused_at) VALUES ($1,$2,'google','chat',true,now())")
            .bind(account).bind(alice).execute(&pool).await.unwrap();
        use crate::workers::google_chat_sweep::mailbox_history_allowed;
        assert!(!mailbox_history_allowed(&pool,account,alice).await.unwrap());
        assert!(mailbox_history_allowed(&pool,account,bob).await.unwrap());
        sqlx::query("UPDATE connector_sources SET paused_at = NULL, enabled = false WHERE account_id = $1").bind(account).execute(&pool).await.unwrap();
        assert!(!mailbox_history_allowed(&pool,account,alice).await.unwrap());
        sqlx::query("UPDATE connector_sources SET enabled = true WHERE account_id = $1").bind(account).execute(&pool).await.unwrap();
        assert!(mailbox_history_allowed(&pool,account,alice).await.unwrap());

        sqlx::query("INSERT INTO oauth_tokens(account_id, owner_user_id, provider, scope, access_token) VALUES ($1,$2,'google_chat','chat','\\x00')")
            .bind(account).bind(alice).execute(&pool).await.unwrap();
        use crate::workers::google_chat_sweep::mailbox_capture_gate;
        let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let old_kek = std::env::var_os("MASTER_KEK_HEX");
        unsafe { std::env::set_var("MASTER_KEK_HEX", "1".repeat(64)); }
        let kek = coralswarm_core::kek::load_kek().unwrap();
        let wrapped = coralswarm_core::kek::wrap_dek(&kek, &coralswarm_core::kek::generate_dek()).unwrap();
        sqlx::query("UPDATE accounts SET dek_wrapped=$1 WHERE id=$2")
            .bind(wrapped).bind(account).execute(&pool).await.unwrap();
        let baseline_item = NormalizedItem {
            source_type: SOURCE_TYPE.into(), source_id: "spaces/shared/messages/before".into(),
            content: "before pause".into(), participants: vec![], timestamp: Utc::now(),
            scope: None, thread_id: None, channel_id: Some("spaces/shared".into()),
            author_id: None, author_name: None,
        };
        let (grant, _) = crate::ingest::OrgSharedGrant::google_chat_space(
            &pool, &pool, &crate::tenancy::TenantId::Consumer, account,
            crate::routes::slack::AccountPlane::Personal, "spaces/shared", Some(alice),
        ).await.unwrap().expect("fixture route");
        let baseline = ingest_space_items(&crate::test_app_state(), &crate::tenancy::TenantId::Consumer,
            &pool, &grant, "test", Some(alice), &[baseline_item]).await;
        assert!(baseline.fully_succeeded(), "fixture must be capable of actual capture");
        let captured: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
            .bind(graph).fetch_one(&pool).await.unwrap();
        assert!(captured > 0);
        for disconnect in [false, true] {
            let gate = mailbox_capture_gate(&pool, account, alice).await.unwrap().unwrap();
            let writer = pool.clone();
            let action = tokio::spawn(async move {
                if disconnect {
                    sqlx::query("DELETE FROM oauth_tokens WHERE account_id=$1 AND owner_user_id=$2")
                        .bind(account).bind(alice).execute(&writer).await.unwrap();
                } else {
                    sqlx::query("UPDATE connector_sources SET paused_at=now() WHERE account_id=$1 AND owner_user_id=$2")
                        .bind(account).bind(alice).execute(&writer).await.unwrap();
                }
            });
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid <> pg_backend_pid())")
                        .fetch_one(&pool).await.unwrap();
                    if blocked { break; }
                    tokio::task::yield_now().await;
                }
            }).await.expect("control action must wait for fetched page commit");
            assert!(!action.is_finished());
            upsert_space_sync_columns(&pool, account, "spaces/shared", Some(alice), Some("2026-10-30T01:00:00Z"), None, true, true).await.unwrap();
            gate.rollback().await.unwrap();
            action.await.unwrap();
            assert!(mailbox_capture_gate(&pool, account, alice).await.unwrap().is_none(), "post-ack fetched page must be rejected");
            let state = std::sync::Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
            for items in [vec![], vec![NormalizedItem {
                source_type: SOURCE_TYPE.into(), source_id: "spaces/shared/messages/late".into(),
                content: "late fetched response".into(), participants: vec![], timestamp: Utc::now(),
                scope: None, thread_id: None, channel_id: Some("spaces/shared".into()),
                author_id: None, author_name: None,
            }]] {
            let fetched = PendingSync { items, filtered: 0,
                commit: PendingCommit::CatchUpComplete("2026-10-30T02:00:00Z".into()) };
            let space = IngestableSpace { space_id: "spaces/shared".into(), mode: Some("single".into()),
                reader_user_id: Some(alice), display_name: "test".into(),
                history_state: "ready".into(), backfill_complete: true };
            assert!(crate::workers::google_chat_sweep::apply_mailbox_pending(
                &state, &pool, &pool, &crate::tenancy::TenantId::Consumer, account, alice, &space, graph, fetched,
            ).await.unwrap().is_none());
            assert_eq!(load_space_sync_state(&pool, account, "spaces/shared", Some(alice)).await.unwrap().unwrap().sync_token.as_deref(), Some("2026-10-30T01:00:00Z"));
            }
            let after: i64 = sqlx::query_scalar("SELECT count(*) FROM source_versions WHERE graph_id=$1")
                .bind(graph).fetch_one(&pool).await.unwrap();
            assert_eq!(after, captured, "post-ack response must not capture");
            if !disconnect {
                sqlx::query("UPDATE connector_sources SET paused_at=NULL WHERE account_id=$1")
                    .bind(account).execute(&pool).await.unwrap();
            }
        }
        unsafe {
            if let Some(value) = old_kek { std::env::set_var("MASTER_KEK_HEX", value); }
            else { std::env::remove_var("MASTER_KEK_HEX"); }
        }

    }

    #[tokio::test]
    async fn mailbox_archetype_check_constraint_forbids_fanout_and_space_type_with_an_owner_pg() {
        // "No fan-out under any circumstance" (this train's own instruction)
        // and the archetype CHECK's SPACE-with-an-owner arm, pinned directly
        // against the DATABASE constraint (migration 0198) rather than only
        // this module's own code paths — so a future writer that bypasses
        // `record_mailbox_optin` entirely still cannot produce either shape.
        let Some(base) = base_url() else {
            eprintln!("skipping mailbox_archetype_check_constraint_forbids_fanout_and_space_type_with_an_owner_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g45_archetype_check";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (owner, _app, _url) = app_role_db(&base, db, false).await;

        let alice = seed_user(&owner).await;
        let account_id = seed_account(&owner, alice).await;
        let graph_id = seed_primary_graph(&owner, account_id).await;

        // A mailbox row can NEVER be 'fanout' — the archetype CHECK requires
        // `mode = 'single'` whenever `owner_user_id IS NOT NULL`.
        let fanout_attempt = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/no-fanout', 'DIRECT_MESSAGE', 'fanout', NULL, $2, $2)",
        )
        .bind(account_id)
        .bind(alice)
        .execute(&owner)
        .await;
        assert!(
            fanout_attempt.is_err(),
            "a mailbox row must NEVER be writable as 'fanout' — got {fanout_attempt:?}"
        );

        // Nor can a mailbox row be 'fanout' with NO graph_id filled in some
        // other way — every disjunct of BOTH check constraints must reject
        // this combination, not just one.
        let fanout_no_owner_check_alone = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/no-fanout-2', 'GROUP_CHAT', 'fanout', NULL, $2, $2)",
        )
        .bind(account_id)
        .bind(alice)
        .execute(&owner)
        .await;
        assert!(fanout_no_owner_check_alone.is_err());

        // A `SPACE` row can never carry an owner — the L4/L7-tightened half
        // of the same constraint (migration 0198's header: the ORIGINAL
        // version of this CHECK short-circuited true for ANY `SPACE` row
        // regardless of `owner_user_id`).
        let space_with_owner = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/space-with-owner', 'SPACE', 'single', $2, $3, $3)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(alice)
        .execute(&owner)
        .await;
        assert!(
            space_with_owner.is_err(),
            "a SPACE row must never carry an owner_user_id — got {space_with_owner:?}"
        );

        // The one shape that IS legal: `mode = 'single'`, owner set, graph set.
        let legal = sqlx::query(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, owner_user_id, reader_user_id) \
             VALUES ($1, 'spaces/legal-mailbox', 'DIRECT_MESSAGE', 'single', $2, $3, $3)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(alice)
        .execute(&owner)
        .await;
        assert!(legal.is_ok(), "the one legal mailbox shape must be writable — got {legal:?}");

        drop_db(&base, db).await;
    }
}

#[cfg(test)]
mod bounded_history_tests {
    use super::*;
    #[tokio::test]
    async fn bounded_history_provider_filters_and_preserves_continuation() {
        use axum::{routing::get, Json, Router};
        let until = Utc::now();
        let bounds = HistoryCursor::new(until, 7);
        let old = (until - chrono::Duration::days(8)).to_rfc3339();
        let current = (until - chrono::Duration::days(2)).to_rfc3339();
        let router = Router::new().route("/spaces/test/messages", get(move |axum::extract::Query(q): axum::extract::Query<HashMap<String,String>>| {
            let old = old.clone(); let current = current.clone();
            async move {
                assert!(q["filter"].contains("createTime >="));
                let resumed = q.contains_key("pageToken");
                let count = if resumed { 1 } else { SYNC_PASS_BUDGET };
                let messages: Vec<_> = (0..count).map(|i| serde_json::json!({"name":format!("spaces/test/messages/{i}"), "text":"hello", "createTime": if resumed { &current } else { &old }, "sender":{"name":"users/person","type":"HUMAN"}})).collect();
                Json(serde_json::json!({"messages":messages,"nextPageToken":if resumed {None} else {Some("next")}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener,router).await.unwrap() });
        TEST_CHAT_API_BASE.scope(base, async {
            let http = reqwest::Client::new();
            let first = fetch_backfill_pass(&http,"token","spaces/test",Some(bounds.encode()),"users/bot",&HashMap::new()).await.unwrap();
            assert!(first.items.is_empty()); assert!(!first.complete);
            let stored = first.resume_cursor.unwrap();
            let decoded: HistoryCursor = serde_json::from_str(stored.strip_prefix("history-v1:").unwrap()).unwrap();
            assert_eq!(decoded.since,bounds.since); assert_eq!(decoded.until,bounds.until);
            let second = fetch_backfill_pass(&http,"token","spaces/test",Some(stored),"users/bot",&HashMap::new()).await.unwrap();
            assert!(second.complete); assert_eq!(second.items.len(),1);
        }).await;
        server.abort();
        assert_eq!(HistoryCursor::new(until,90).since, until - chrono::Duration::days(28));
    }
}
