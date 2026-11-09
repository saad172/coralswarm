//! Connector framework — a per-provider strategy layer over the generic OAuth
//! routes (`routes/oauth.rs`) and the ingest pipeline (`routes/ingest.rs`).
//!
//! # Why this exists
//!
//! Connectors used to be bespoke copy-from-Slack: the OAuth `authorize`/`callback`
//! handlers hardcoded `match provider.as_str()` arms and inlined token-exchange,
//! DEK derivation, encryption, the `oauth_tokens` upsert, and Slack-specific side
//! effects. Adding a provider meant duplicating all of it.
//!
//! This module factors out the *generic* parts (DEK + encryption + storage, done
//! once in `routes/oauth.rs::store_token_bundle`) from the *provider-specific*
//! parts (consent URL, code exchange, token refresh, pull-sync), which live behind
//! the [`ConnectorProvider`] trait. A new provider is now ~1 file: implement the
//! trait and register it in [`lookup`].
//!
//! # Registry semantics (preserved from the old `match` arms)
//!
//! - **unknown provider** → [`lookup`] returns `None` → routes answer `404`.
//! - **registered but unimplemented** (e.g. `google`) → the connector returns
//!   [`ConnectorError::NotImplemented`] → routes answer `501`.
//! - **registered, implemented, but env missing** → [`ConnectorError::NotConfigured`]
//!   → routes answer `503`.
//! - **upstream exchange failure** → [`ConnectorError::Exchange`] (no HTTP status) or
//!   [`ConnectorError::UpstreamStatus`] (the provider's non-2xx) → routes answer `502`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

// `pub(crate)` (gsuite Train G1 PR 1) so `routes::oauth::authorize` can reach
// `google::Surface`/`google::authorize_url_for_surface` to gate and build the
// per-surface consent URL — same reason `microsoft`/`slack` below are
// `pub(crate)`.
pub(crate) mod composio;
pub(crate) mod google;
pub(crate) mod google_transport;
// Calendar ingest (gsuite Train G3 PR 1): events → atoms via `sync()`, watch
// (channel) registration, incremental sync-token/backfill state, and
// deletion propagation. Kept out of `google.rs` (already large) as its own
// module; `pub(crate)` so `routes::google_calendar_push` (the Calendar
// webhook, a separate route from the Pub/Sub `routes::google_push`) can
// reach [`google_calendar::sync`] and its account-owner/graph helpers.
pub(crate) mod google_calendar;
// Gmail ingest (gsuite Train G2 PR 3): `users.watch`, `history.list`
// incremental sync, opt-in per-thread fetch/live-follow, and deletion
// propagation. `pub(crate)` so `routes::google_push` (the Pub/Sub push
// webhook) can reach its `fetch_pending`/`commit_pending`/
// `resolve_watch_owners_for_push` — same reason `google_calendar` above is
// `pub(crate)`.
pub(crate) mod google_gmail;
pub(crate) mod google_workspace;
// Shared email/HTML body normalisation (gsuite Train G2 PR 2): the
// tag-stripping core `microsoft.rs`'s Teams path used to own privately, plus
// quoted-reply/signature trimming and Gmail MIME-payload shaping. `pub(crate)`
// so `microsoft` (below) and the future `google_gmail` (Train G2 PR 3) can
// both reach it — same reason `google_calendar` above is `pub(crate)`.
pub(crate) mod email_body;
// Google Chat message normalisation + the `spaceType` archetype
// discriminator (gsuite Train G4 PR 2 — design doc §1.6/§3.6/§3.7). Its own
// sibling module rather than an `email_body.rs` adapter: a Chat `Message` is
// not MIME/HTML/quoted-reply shaped, so the only overlap is `html_to_text`,
// reachable via a one-line `use` (not exercised yet — see the module doc).
// `pub(crate)` for the same "the future connector module (Train G4 PR 4)
// needs to reach it" reason `google_gmail`/`google_calendar` above are.
pub(crate) mod chat_body;
// Google Chat watched-space model (gsuite Train G4 PR 3 — design doc
// §3.1/§3.2/§5.2): the app-install gate (`ADDED_TO_SPACE`/
// `REMOVED_FROM_SPACE`), the per-space destination picker, and the Chat API
// calls (`spaces.get`/`spaces.list`/`spaces.members.list`) they need.
// `pub(crate)` so `routes::google_push` (the Chat-app-interaction-event
// arm) and the new `routes::google_chat` routes can both reach it — same
// reason `google_gmail`/`chat_body` above are `pub(crate)`. A *surface* of
// the existing `GoogleConnector` (`connectors::google::Surface::Chat`), not
// a new top-level provider — `ALL`/`lookup` below are unchanged (design doc
// §1.11's `connectors/mod.rs` registration note).
pub(crate) mod google_chat;
pub(crate) mod google_chat_invite;
pub(crate) mod chat_app_identity;
pub(crate) mod chat_cold_start;
// Google Meet ingest (gsuite Train G3 PR 2 — `docs/connectors/
// google-workspace.md` §11, contract §8 Meetings archetype): conference
// record / transcript / participant API shapes, per-attendee identity
// resolution, and the fan-out ingest call. `pub(crate)` so
// `workers::google_meet_sweep` (the tenant-loop pull driver) can reach
// [`google_meet::sync_owner_once`] — same reason `google_chat`/`google_gmail`
// above are `pub(crate)`.
pub(crate) mod google_meet;
pub(crate) mod google_meet_events;
// GitHub App connector (PR A, sync plumbing — docs/connectors/github.md).
// `pub(crate)` so `routes::github` (the selection/webhook HTTP surface) and
// `routes::oauth::callback` (the install-recording call after a GitHub
// OAuth completion) can reach its App-JWT minting, installation-token
// cache, REST client, and `connector_installs`/`github_installations`
// bookkeeping — same reason every other provider module above is
// `pub(crate)`.
pub(crate) mod github;
// GitHub repo selection / destination-gating logic (PR A): the per-repo
// personal/team/org destination picker (§3.4) and repo-mirrored team
// creation + auto-seat. Kept out of `github.rs` (already large — App JWT +
// REST client + ConnectorProvider impl) as its own module; `pub(crate)` so
// `routes::github` can reach it, same reason `github` above is `pub(crate)`.
pub(crate) mod github_repos;
// The §8 conformance suite (`CONNECTOR_CONTRACT.md` §6's required
// deliverable). Test-only, and in-crate rather than under `backend/tests/`
// because every function it certifies is `pub(crate)` — see its own module
// doc for the full reasoning and the table of which item is pinned where.
#[cfg(test)]
mod github_conformance;
// The GitHub analog of `slack_disconnect`/`microsoft_disconnect`/
// `google_disconnect` — but PAUSE-ONLY (docs/connectors/github.md §3.8:
// "never purge"), never a delete. `pub(crate)` for the same "the calling
// route needs to reach it" reason its siblings above are.
pub(crate) mod github_disconnect;
// PR A -> PR B "sync plumbing" -> real ingest (phase 2): fetchers +
// normalisers for the four narrative source types (§3.5). `pub(crate)` so
// `connectors::github_backfill`/`connectors::github_event_ingest` and the
// conformance tests can reach it.
pub(crate) mod github_narrative;
// The real, resumable, per-repo, per-type-cursored narrative backfill engine
// (§3.6) — `workers::github_backfill` is the thin tenant-loop wrapper that
// dequeues `JobKind::GithubBackfill` and calls into this module.
pub(crate) mod github_backfill;
// The real per-webhook-event dispatcher (§3.7) — `workers::
// github_event_ingest` is the thin tenant-loop wrapper that dequeues
// `JobKind::GithubEventIngest` and calls into this module; the reconcile
// sweep (`workers::github_reconcile_sweep`) calls the SAME handlers.
pub(crate) mod github_event_ingest;
// No code-tier modules: the connector captures no repository source code
// (founder decision 2026-10-27, issue #1516). `github_code`,
// `github_code_filter`, `github_code_groups` and `github_summary` were
// deleted; existing `github_code`/`github_code_summary` atoms are kept.
// `pub(crate)` so the change-notification webhook and renewal worker can reach
// the Graph subscription helpers (`microsoft::renew_subscription`, etc.).
pub(crate) mod microsoft;
/// Microsoft Calendar ingest via Graph `calendarView/delta` — the Microsoft
/// twin of `google_calendar`: persisted cursor, deletions/cancellations
/// retracted, recurring series stored as one coral. See its module doc.
pub(crate) mod microsoft_calendar;
// Teams channel/chat push is one doorbell per (tenant, resource) on the
// control plane (Phase G wave 3, item 56). `pub(crate)` so the Graph webhook
// and the delegated watch routes can reach it.
pub(crate) mod teams_doorbell;
// Per-member Teams transcript capture (Phase G item 51). Delegated organiser
// push beside the RSC `getAllTranscripts` stream, which item 48 retires later.
pub(crate) mod teams_transcripts;
pub(crate) mod teams_targeted_transcripts;
pub(crate) mod outlook_schedule;
// `pub(crate)` so the Slack Events API webhook funnel (`routes::slack_events`)
// can reach the per-channel destination helpers
// (`resolve_channel_destinations`/`slack_watched_destination`/
// `ensure_slack_watched_channel`) — same reason `microsoft` is `pub(crate)`
// above.
pub(crate) mod slack;
pub(crate) mod slack_huddle_consent;
pub(crate) mod cold_start;
// Per-member Slack capture (Phase G wave 3, item 46b): channel index and
// doorbell ingest on `slack_user` tokens. Does not replace the org fan-out
// path (item 48).
pub(crate) mod slack_member_capture;
// The one server-derived Slack onboarding status payload
// (docs/connectors/slack-onboarding.md §3), shared by `routes::slack::
// onboarding_status` (JSON) and `routes::slack_events`' App Home renderer
// (Block Kit) — `pub(crate)` so both call sites can reach it, same reason
// `slack` above is `pub(crate)`.
pub(crate) mod slack_onboarding;
// Flattening one Slack message object (`text` + `attachments[]` + `blocks[]`)
// into the single plain-text body the ingest pipeline stores — the pure core
// of "ingest bot-authored messages". `pub(crate)` so BOTH ingest funnels
// (`routes::slack_events`' live webhook and `routes::slack_interactivity`'s
// `backfill_channel`) call the identical rules, same reason `slack` above is
// `pub(crate)`.
pub(crate) mod slack_message_text;
pub(crate) mod slack_review;
// The per-channel Slack management payload (`GET /v1/connectors/slack/
// channels`) — docs/connectors/slack-onboarding.md §3's rules, generalized to
// one row per channel. `pub(crate)` so `routes::slack::channel_management` can
// reach it, same reason `slack_onboarding` above is `pub(crate)`.
pub(crate) mod slack_channel_management;
// The WRITE half of Slack channel management (`PUT .../destination`,
// `DELETE .../channels/{id}`, `GET .../impact`) — change-destination /
// stop-watching / preview-before-you-click, sitting next to
// `slack_channel_management`'s read side. `pub(crate)` so
// `routes::slack`'s new handlers can reach it, same reason
// `slack_channel_management` above is `pub(crate)`.
pub(crate) mod slack_channel_actions;
// The shared disconnect/purge routine (org governance decision 2026-08-04,
// piece 3) — one code path for "stop watching this Slack workspace",
// called from `routes::slack::delete_integration`, `routes::oauth::
// delete_connection`'s Slack arm, and `routes::slack_events`'
// `app_uninstalled`/`tokens_revoked` handlers. `pub(crate)` so all three can
// reach it, same reason `slack_channel_actions` above is `pub(crate)`.
pub(crate) mod slack_disconnect;
// Recipient discovery for "email the unmatched" (chat-per-user plan PR 6):
// walks a channel's roster (the SAME `resolve_channel_members`-style walk,
// reusing `resolve_slack_identity_with_email`) and returns the verified
// email addresses of members who are `Unmatched` — never `Unavailable`/`Bot`/
// `Matched`. CS-426 (D2, D21) narrows that further: an `Unmatched` guest or
// Slack Connect participant is only collected when their email domain
// matches the account's anchor domain (`connectors::slack::resolve_anchor_
// domain`) — see this module's own doc comment for the full rule.
// `pub(crate)` so `workers::slack_unmatched_email` can reach it, same reason
// every sibling `slack_*` module above is `pub(crate)`.
pub(crate) mod slack_unmatched_email;
// The Microsoft analog of `slack_disconnect` (m365 Train 3 PR 3.7) — one code
// path for "stop watching this Microsoft connection", called from
// `routes::oauth::delete_connection`'s `microsoft` arm. `pub(crate)` for the
// same "the calling route needs to reach it" reason `slack_disconnect` above
// is `pub(crate)`.
pub(crate) mod microsoft_disconnect;
// The Google analog of `microsoft_disconnect` (gsuite Train G2 PR 5, debt) —
// one code path for "stop watching this Google connection" (Gmail or
// Calendar), called from `routes::oauth::delete_connection`'s `google_gmail`/
// `google` arms. `pub(crate)` for the same "the calling route needs to reach
// it" reason `microsoft_disconnect` above is `pub(crate)`.
pub(crate) mod google_disconnect;
// Member offboarding (settings launch PR 2.8): revoke a departed member's
// org-scoped credentials, installs and watches, and the live-membership gate
// every capture path checks before writing. Called from
// `clerk_provision::remove_org_membership` and from the capture gates.
pub(crate) mod member_offboarding;
// Operational-state builder (CoralSwarm self-knowledge) — see state.rs.
pub(crate) mod state;
// Provider-agnostic history backfill engine (chat-per-user-ingest plan §4,
// PR 2) — the Slack arm is implemented; Teams/Google Chat slot in later.
// `pub(crate)` so `workers::history_backfill` (the durable queue consumer)
// and `routes::slack_interactivity` (the enqueuer) can both reach it.
pub(crate) mod history_backfill;
// Credential-death classification + the durable `oauth_tokens.
// credential_invalid_at`/`credential_invalid_reason` set/clear helpers
// (CS-453, Phase G wave 1, D21 — `docs/plans/org-onboarding-from-the-invite.md`
// §3). `pub(crate)` so `connectors::microsoft`/`connectors::slack`/
// `connectors::google` (which raise [`ConnectorError::CredentialRevoked`]),
// `routes::connectors` and `workers::google_calendar_sync` (which observe it
// off a `sync`/`fetch_items`/pass call) and `routes::slack_events` (the
// `tokens_revoked` event arm) can all reach it — same reason every sibling
// module above is `pub(crate)`.
pub(crate) mod credential_health;
// The generic connector catalog (connectors redesign, layer B1): one
// `ConnectorAdapter` per connector over the provider code above, the one status
// derivation, and the wire `ConnectorSummary`. `routes::connector_catalog` serves it.
pub(crate) mod catalog;

pub use github::GithubConnector;
pub use google::GoogleConnector;
pub use microsoft::MicrosoftConnector;
pub use slack::SlackConnector;

// ---------------------------------------------------------------------------
// Normalized token bundle — provider-agnostic
// ---------------------------------------------------------------------------

/// The result of a provider's OAuth code exchange (or token refresh), normalized
/// so the generic store path (`routes/oauth.rs::store_token_bundle`) can persist
/// it without knowing anything about the provider.
#[derive(Debug, Clone)]
pub struct TokenBundle {
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// A second, user-scoped token some providers issue alongside the primary
    /// (e.g. Slack's `authed_user.access_token`). Stored in `user_token_encrypted`.
    pub user_token: Option<String>,
    pub scope: String,
    /// Absolute expiry of `access_token`, when the provider reports one.
    pub expires_at: Option<DateTime<Utc>>,
    /// Provider-native workspace/tenant id — part of the `oauth_tokens` unique key
    /// (Slack `team_id`, Microsoft tenant `tid`).
    pub provider_workspace_id: String,
    /// Human-facing workspace/tenant name, for provider-specific side tables and UI.
    pub provider_workspace_name: Option<String>,
    /// Provider-native user id (Slack `authed_user.id`, Microsoft `oid`).
    pub provider_user_id: String,
}

// ---------------------------------------------------------------------------
// Owner selector — the `oauth_tokens.owner_user_id` dimension (Foundation PR 1.1)
// ---------------------------------------------------------------------------

/// Explicit selector for which owner dimension of `oauth_tokens` a
/// read/write targets — the Foundation remodel's `owner_user_id` column
/// (control migration `0175_oauth_tokens_owner_user_id`, tenant `0059`;
/// nullable, no backfill). Implements the design drafted in
/// `docs/plans/org-connectors-and-channel-ingest.md` §10.
///
/// `oauth_tokens` now carries two disjoint partial unique indexes instead of
/// the old single 3-column constraint: `uq_oauth_tokens_account_wide`
/// (`account_id, provider, provider_workspace_id` WHERE `owner_user_id IS
/// NULL`) and `uq_oauth_tokens_per_owner` (the same three columns plus
/// `owner_user_id`, WHERE it IS NOT NULL). Every read/write against
/// `oauth_tokens` must pick exactly one of the two shapes — **there is
/// deliberately no `owner_user_id IS NULL OR owner_user_id = $n` fallback
/// anywhere** in this codebase. A fallback would silently hand a member the
/// account-wide token, which is precisely the cross-member credential leak
/// this type exists to make structurally impossible (see
/// `routes::oauth::org_slack_oauth_enabled`'s own doc comment for the
/// concrete incident shape this closes).
///
/// Originally every caller passed [`TokenOwner::AccountWide`] (Foundation PR
/// 1.1 shipped this type as pure forward-compat plumbing, no behavior
/// change). That is no longer true fleet-wide: gsuite Train G2 PR 1 made
/// Google Gmail unconditionally `Member`-owned, and m365 enterprise train PR
/// 3.2 gives Microsoft an org-plane `Member` branch too (see
/// `routes::oauth::resolve_microsoft_token_target` — in practice still
/// unreachable until the org/provider callback gate lifts for Microsoft,
/// Train 3.3+, but no longer merely a test fixture). Slack and Google
/// Calendar remain `AccountWide`-only by deliberate design, not by gap — see
/// `connectors::google_calendar`'s own module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenOwner {
    /// `owner_user_id IS NULL` — one token shared by the whole account: bot
    /// tokens (Slack), Google Calendar (by design), and Microsoft on the
    /// personal plane (today, unconditionally — see
    /// `routes::oauth::resolve_microsoft_token_target`'s own doc for why).
    AccountWide,
    /// `owner_user_id = <uuid>` — a per-member token. Real, live-reachable
    /// production use: Google Gmail, unconditionally, as of gsuite Train G2
    /// PR 1 (`connectors::google::Surface::token_owner_for`). Forward-compat,
    /// not-yet-reachable plumbing: Microsoft on the org plane, as of m365
    /// enterprise train PR 3.2 (`routes::oauth::resolve_microsoft_token_target`).
    /// Both are constructed by `routes::oauth::callback` strictly from the
    /// connecting user id embedded in the verified OAuth `state` token —
    /// never a client parameter.
    Member(Uuid),
}

impl TokenOwner {
    /// The `owner_user_id` value to bind on a write: `None` for
    /// `AccountWide` (the column itself), `Some(id)` for `Member`. First real
    /// production use (gsuite Train G2 PR 1, CodeRabbit round 1 fix):
    /// `routes::oauth::record_connector_install` binds this alongside
    /// `oauth_tokens`' own `owner_user_id` so a Gmail connection's
    /// `connector_installs` row carries the SAME owner dimension as the
    /// token row it mirrors — see that function's own doc comment.
    pub fn owner_user_id(&self) -> Option<Uuid> {
        match self {
            TokenOwner::AccountWide => None,
            TokenOwner::Member(id) => Some(*id),
        }
    }
}

// ---------------------------------------------------------------------------
// Decrypted credentials handed to a sync run
// ---------------------------------------------------------------------------

/// Plaintext credentials passed to [`ConnectorProvider::sync`]. The sync route
/// decrypts the stored tokens into this; if the provider refreshes the access
/// token mid-run it sets [`refreshed`](Self::refreshed) so the route re-encrypts
/// and updates `oauth_tokens`.
#[derive(Debug)]
pub struct ConnectorCredentials {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    /// The connected workspace/tenant id. Microsoft's `/me` calls don't need it,
    /// but workspace-scoped providers (e.g. a future Slack pull path) do.
    #[allow(dead_code)]
    pub provider_workspace_id: String,
    /// Set by the provider when it obtains a fresh token during sync. The route
    /// persists this (re-encrypted) so subsequent syncs start from the new token.
    pub refreshed: Option<TokenBundle>,
    /// `oauth_tokens.updated_at` of the row these were loaded from, advanced
    /// to the value the holder's own persist returned when it refreshed them
    /// (`routes::oauth::store_token_bundle`). The witness
    /// `credential_health::record_if_revoked_since` hands to the
    /// `credential_invalid_at` write: a row written after this instant — a
    /// reconnect that landed while a pass was still failing on the old token
    /// — is never marked by that pass. `None` when the credentials did not
    /// come from a row (no guard; today's unconditional mark).
    pub row_updated_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// A normalized item ready to become a coral (atom)
// ---------------------------------------------------------------------------

/// One pulled item from a provider, normalized into the fields the ingest
/// pipeline (`routes/ingest.rs::IngestAtomRequest`) needs. The sync route adds
/// `graph_id` (the destination ocean) and forwards each to `ingest_atom`.
#[derive(Debug, Clone)]
pub struct NormalizedItem {
    /// Atom `source_type` — drives chunking (`ChunkStrategy::for_source_type`):
    /// e.g. `"outlook"` (email chunker), `"microsoft_calendar"` (generic).
    pub source_type: String,
    /// Provider-native id, unique within `(graph_id, source_type)`.
    pub source_id: String,
    pub content: String,
    pub participants: Vec<String>,
    pub timestamp: DateTime<Utc>,
    /// Visibility: `Some("account")` or `None`/`Some("private")` (default).
    pub scope: Option<String>,
    /// Provider-native thread/conversation id this item belongs to, when the
    /// provider groups items into threads (Outlook `conversationId`). Lets the
    /// ingest route register the thread for auto-capture of future replies
    /// (CS-117). `None` for un-threaded items (e.g. calendar events).
    pub thread_id: Option<String>,
    /// Provider-native container id (Teams channel/chat id) this item belongs
    /// to. Stamped onto the atom's `channel_id` so the source can be labelled
    /// with a human name rather than the raw container GUID. `None` for sources
    /// without a channel concept (email, calendar).
    pub channel_id: Option<String>,
    /// Provider-native author id (Teams/AAD user GUID). Stamped onto the atom's
    /// `author_id` so the same human stays anchored across messages even when
    /// their display name is missing. `None` when the source carries no author.
    pub author_id: Option<String>,
    /// Human-readable author display name. Stamped onto the atom's `author_name`.
    /// `None` when unavailable (then the id, if any, is the only handle).
    pub author_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Error type — each variant maps to a precise HTTP status in the route layer
// ---------------------------------------------------------------------------

/// A connector operation failure. The route layer maps each variant to the HTTP
/// status the old hardcoded handlers returned (see module docs).
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Required env (client id/secret, scopes) is absent. → `503`.
    #[error("connector not configured: {0}")]
    NotConfigured(String),
    /// The provider is registered but intentionally not implemented. → `501`.
    #[error("not implemented: {0}")]
    NotImplemented(String),
    /// The provider's upstream API failed with no HTTP status to decide on:
    /// a network error, a timeout, a response that did not parse, or a
    /// failure that is not one HTTP response (an aggregate, a refused
    /// request). → `502`. A non-2xx answer is [`UpstreamStatus`](Self::
    /// UpstreamStatus) instead, so the status travels as a field, not as
    /// text to be scraped back out of the message.
    #[error("upstream exchange failed: {0}")]
    Exchange(String),
    /// The provider answered a non-2xx HTTP status. → `502`. `status` is
    /// what the provider answered, verbatim; `message` is the same bounded
    /// text an [`Exchange`](Self::Exchange) would carry (`"<what> failed:
    /// <status>: <body snippet>"`). Built with [`ConnectorError::upstream`]
    /// where each provider client reads the response. Read with
    /// [`ConnectorError::upstream_status`] by whoever classifies the
    /// failure (`workers::ingest_receive::transient`, `connectors::
    /// teams_doorbell::classify_patch_failure`, `microsoft::
    /// subscription_slot_taken`).
    #[error("upstream exchange failed: {message}")]
    UpstreamStatus { status: u16, message: String },
    /// An internal failure inside the connector (e.g. a DB write in a
    /// post-ingest side effect). → `500`. Surfaced mainly via logs since the
    /// operations that raise it (`after_ingest`) are best-effort.
    #[error("connector internal error: {0}")]
    Internal(String),
    /// This exact credential is DEAD, not merely unreachable — a DEFINITE
    /// signal (token revoked, consent revoked, account disabled/deleted, or
    /// a forced re-auth) rather than an upstream hiccup. → `409` on
    /// authenticated connector/teams/calendar routes (not `401`: that status
    /// is Clerk-session recovery in `web/lib/api.ts`). CS-453 (Phase G wave
    /// 1, D21): distinguishes "prompt this member to reconnect" from
    /// [`Exchange`](Self::Exchange)'s "retry, it'll probably come back" —
    /// see each provider's `classify_*` function (`connectors::microsoft::
    /// classify_microsoft_token_error`, `connectors::slack::
    /// classify_slack_api_error`, `connectors::google::
    /// classify_google_token_error`) for the exact, deliberately narrow signal
    /// list each is raised from. Never raised from a timeout, a 5xx, a 429,
    /// or a plain network error — D16's "could not ask ≠ does not work"
    /// (`connector_health`'s migration `0243` header) applies here too.
    #[error("credential revoked ({provider}): {reason}")]
    CredentialRevoked {
        /// The `oauth_tokens.provider` literal this credential lives under
        /// (`"microsoft"`, `"slack"`, `"slack_user"`, `"google"`) — NOT necessarily
        /// `ConnectorProvider::id()` (Gmail's per-surface provider key is the
        /// same distinction `ConnectorProvider::fetch_items_credentials`
        /// documents).
        provider: &'static str,
        /// The specific signal this was classified from — also the value
        /// `connectors::credential_health::mark_credential_invalid` persists
        /// into `oauth_tokens.credential_invalid_reason` (migration `0251`).
        reason: CredentialDeathReason,
    },
}

impl ConnectorError {
    /// A non-2xx answer from the provider, as
    /// [`UpstreamStatus`](Self::UpstreamStatus).
    pub fn upstream(status: reqwest::StatusCode, message: impl Into<String>) -> Self {
        ConnectorError::UpstreamStatus { status: status.as_u16(), message: message.into() }
    }

    /// [`upstream`](Self::upstream) for a status already reduced to its
    /// number (a provider error type that carries it as `u16`).
    pub fn upstream_code(status: u16, message: impl Into<String>) -> Self {
        ConnectorError::UpstreamStatus { status, message: message.into() }
    }

    /// The HTTP status an upstream failure carried: `Some` only for
    /// [`UpstreamStatus`](Self::UpstreamStatus). `None` for a network error,
    /// a timeout, a response that did not parse, or any other variant: the
    /// caller then has no status to decide on and treats the failure as
    /// transient. Nothing is read back out of the message text.
    pub fn upstream_status(&self) -> Option<u16> {
        match self {
            ConnectorError::UpstreamStatus { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The provider-facing message of an upstream failure, for the few
    /// classifiers that must read a provider's error code out of the body
    /// snippet (Graph's `GraphAccessToTranscriptsDisabled`, `"already
    /// exists"`): `Some` for [`Exchange`](Self::Exchange) and
    /// [`UpstreamStatus`](Self::UpstreamStatus), `None` otherwise.
    pub fn upstream_message(&self) -> Option<&str> {
        match self {
            ConnectorError::Exchange(message) | ConnectorError::UpstreamStatus { message, .. } => Some(message),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Credential-death reason codes (CS-453, Phase G wave 1, D21)
// ---------------------------------------------------------------------------

/// The specific DEFINITE signal a [`ConnectorError::CredentialRevoked`] was
/// classified from. Each variant maps 1:1 to a stable [`code`](Self::code)
/// stored verbatim in `oauth_tokens.credential_invalid_reason` (migration
/// `0251`) — never a free-form message, so a later admin-facing surface can
/// render a fixed copy per reason without parsing prose.
///
/// Deliberately NOT exhaustive of every possible provider error — see
/// `connectors::microsoft::classify_microsoft_token_error`,
/// `connectors::slack::classify_slack_api_error` and
/// `connectors::google::classify_google_token_error`'s own doc comments for the
/// research behind exactly which upstream codes map here and which were
/// left out as ambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialDeathReason {
    /// Microsoft AADSTS50173 — "The provided grant has expired due to it
    /// being revoked, a fresh auth token is needed." The user changed/reset
    /// their password, or an admin reset their credentials/revoked sessions.
    MicrosoftPasswordChangedOrAdminReset,
    /// Microsoft AADSTS50057 — "The user account is disabled. The account
    /// has been disabled by an administrator."
    MicrosoftAccountDisabled,
    /// Microsoft AADSTS50034 — "The user account {identifier} does not
    /// exist in the {tenant} directory." The user was deleted.
    MicrosoftAccountDeleted,
    /// Microsoft AADSTS65001 — "The user or administrator has not consented
    /// to use the application." Observed on a REFRESH (a prior grant already
    /// existed), so this means consent was revoked, not "never granted".
    MicrosoftConsentRevoked,
    /// Microsoft AADSTS70008 (`ExpiredOrRevokedGrant`) — "The provided
    /// authorization code or refresh token has expired due to inactivity."
    /// The refresh token outlived its sliding inactivity window (Microsoft's
    /// default is 90 days). Nothing short of the user signing in again mints
    /// a new one, so the row is as dead as a revoked grant until a reconnect.
    MicrosoftGrantExpired,
    /// Microsoft AADSTS700082 (`ExpiredOrRevokedGrantInactiveToken`) — "The
    /// refresh token has expired due to inactivity. The token was issued on
    /// {date} and was inactive for {time}." Same remedy as 70008 (a
    /// reconnect), kept as its own code so the stored reason says which of
    /// the two Microsoft answered.
    MicrosoftRefreshTokenInactive,
    /// Slack `token_revoked` — the user (or a workspace admin) revoked this
    /// specific token.
    SlackTokenRevoked,
    /// Slack `invalid_auth` — the token is invalid for authentication
    /// (revoked, malformed, or from a since-removed integration).
    SlackInvalidAuth,
    /// Slack `account_inactive` — the token's user or workspace has been
    /// deactivated.
    SlackAccountInactive,
    /// Slack's `tokens_revoked` Events API push (`event.tokens.oauth`) named
    /// this specific user's grant, independent of any API-response-based
    /// classification above — `routes::slack_events`' `tokens_revoked` arm.
    SlackTokensRevokedEvent,
    /// Google `/token` refresh answered `400 invalid_grant` — the refresh
    /// token has been revoked (user removed the app, Workspace admin
    /// revoked it, password change on a sensitive-scope grant) or expired
    /// (testing-mode 7-day tokens, 6-month inactivity). Google issues no
    /// finer code: every other refresh failure (`invalid_client`,
    /// `unauthorized_client`, 5xx) stays `Exchange`. See
    /// `connectors::google::classify_google_token_error`.
    GoogleInvalidGrant,
    /// Google Calendar API answered `401` to an access token minted by a
    /// successful refresh in the SAME pass — the grant died between the
    /// refresh and the call. A 401 on a stored, not-yet-expired token is
    /// NOT classified (a clock-skewed or early-expired access token is
    /// repaired by the next refresh, which then speaks for itself). See
    /// `connectors::google_calendar::fetch_pending`.
    GoogleUnauthorizedAfterRefresh,
}

impl CredentialDeathReason {
    /// The stable string persisted to `oauth_tokens.credential_invalid_reason`
    /// and what other code should match on — never re-derive this from
    /// `Debug`/`Display` (those are for logs, this is a wire/DB contract).
    pub fn code(self) -> &'static str {
        match self {
            Self::MicrosoftPasswordChangedOrAdminReset => "ms_password_changed_or_admin_reset",
            Self::MicrosoftAccountDisabled => "ms_account_disabled",
            Self::MicrosoftAccountDeleted => "ms_account_deleted",
            Self::MicrosoftConsentRevoked => "ms_consent_revoked",
            Self::MicrosoftGrantExpired => "ms_grant_expired",
            Self::MicrosoftRefreshTokenInactive => "ms_refresh_token_inactive",
            Self::SlackTokenRevoked => "slack_token_revoked",
            Self::SlackInvalidAuth => "slack_invalid_auth",
            Self::SlackAccountInactive => "slack_account_inactive",
            Self::SlackTokensRevokedEvent => "slack_tokens_revoked_event",
            Self::GoogleInvalidGrant => "google_invalid_grant",
            Self::GoogleUnauthorizedAfterRefresh => "google_unauthorized_after_refresh",
        }
    }
}

impl std::fmt::Display for CredentialDeathReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

// ---------------------------------------------------------------------------
// Opt-in save options (gmail-options backend train, PR 1 of 4: backend →
// Gmail add-on card → web list → Outlook add-in)
// ---------------------------------------------------------------------------

/// How much of a thread an opt-in save should fetch — the wire-level `scope`
/// knob on [`crate::routes::connectors::IngestSelectedRequest`], threaded
/// through to [`ConnectorProvider::fetch_items`] so each provider can decide
/// whether to do its own thread/conversation expansion.
///
/// `#[serde(rename_all = "lowercase")]` + `#[serde(default)]` at the call
/// site (not here) is what gives the locked "both knobs omitted is
/// byte-identical to today" contract: an absent `scope` field deserializes
/// via [`Default`] to [`Thread`](Self::Thread) — today's only behavior — and
/// an unrecognized string (anything other than `"thread"`/`"message"`) is a
/// serde deserialization failure. `routes::connectors::ingest_selected`
/// explicitly maps that JSON data rejection to `400` (Axum defaults to
/// `422`), never silently falling back to the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IngestScope {
    /// Fetch the whole thread/conversation the selected item(s) belong to
    /// (today's only behavior, and the default) — Gmail's `threads.get`
    /// expansion, Outlook's `fetch_conversation` expansion.
    #[default]
    Thread,
    /// Fetch only the specific item id(s) the caller named — no
    /// thread/conversation expansion at all.
    Message,
}

// ---------------------------------------------------------------------------
// The provider strategy trait
// ---------------------------------------------------------------------------

/// A single OAuth connector. Implementations are zero-sized and stateless — all
/// configuration is read from the environment at call time — so they live as
/// `&'static` values in [`lookup`].
#[async_trait]
pub trait ConnectorProvider: Send + Sync {
    /// Stable id matching the `:provider` path segment and the
    /// `oauth_tokens.provider` column literal (e.g. `"slack"`, `"microsoft"`).
    fn id(&self) -> &'static str;

    /// Human-facing display name. Surfaced by the upcoming connector catalog
    /// endpoint (the web UI currently carries its own labels).
    #[allow(dead_code)]
    fn display_name(&self) -> &'static str;

    /// Build the provider consent URL. `state` is the CSRF/account token chosen
    /// by the caller. Reads env (client id, scopes) at call time.
    fn authorize_url(&self, base_url: &str, state: &str) -> Result<String, ConnectorError>;

    /// Build the *org-wide admin consent* URL, for providers that let a tenant
    /// admin authorize the whole organization in one action (so employees can
    /// connect even when the tenant blocks per-user consent). The admin flow
    /// returns a normal `code` to the standard callback, so no extra handling is
    /// needed beyond this URL. Returns `None` for providers without org-wide
    /// admin consent (e.g. Slack). Default: `None`.
    fn admin_consent_url(
        &self,
        _base_url: &str,
        _state: &str,
    ) -> Option<Result<String, ConnectorError>> {
        None
    }

    /// Exchange an authorization `code` for a normalized [`TokenBundle`].
    async fn exchange_code(
        &self,
        http: &reqwest::Client,
        base_url: &str,
        code: &str,
    ) -> Result<TokenBundle, ConnectorError>;

    /// Optional provider-specific side effect run *after* the generic
    /// `oauth_tokens` upsert succeeds (e.g. Slack's `slack_integrations` write,
    /// and — slack-onboarding PR — Slack's welcome DM to the installing user).
    /// Best-effort: errors are logged, never propagated, so they cannot fail the
    /// OAuth redirect. Default: no-op.
    ///
    /// `http` was added (previously every implementer reached for its own
    /// throwaway `reqwest::Client`, or had no way to make a network call at
    /// all) so a provider can make best-effort API calls here without
    /// constructing a second client — see `SlackConnector::post_store`'s
    /// welcome-DM call for the first real use.
    ///
    /// `provider_key` (gsuite Train G2 PR 1, CodeRabbit round-1 fix): the
    /// EXACT `oauth_tokens.provider` literal [`callback`](crate::routes::oauth::callback)
    /// just stored this bundle under — for a single-surface provider this is
    /// always [`ConnectorProvider::id`], but for a provider with a per-surface
    /// scope split (Google — see `connectors::google::Surface::
    /// token_provider`) it discriminates WHICH surface actually completed,
    /// which `bundle.scope` alone cannot: Google's incremental consent
    /// (`include_granted_scopes=true`) means a member who already granted
    /// Calendar and now connects Gmail gets a token whose `scope` contains
    /// BOTH surfaces' scopes, so a scope-content check would misfire on
    /// every OTHER surface's connect too. A provider without a surface split
    /// simply ignores this parameter (ordinary implementers ignore it the
    /// same way they already ignore `_bundle`/`_pool` when unused).
    async fn post_store(
        &self,
        _http: &reqwest::Client,
        _pool: &sqlx::PgPool,
        _account_id: Uuid,
        _bundle: &TokenBundle,
        _provider_key: &str,
    ) {
    }

    /// The `atoms.source_type` values this connector owns. Drives the
    /// operational-state block (`state.rs`): coverage rows and default container
    /// derivation are keyed on these. Default: none — a connector contributes
    /// nothing to STATE until it declares its source types (forward-compat: an
    /// unbuilt connector like Google stays invisible with zero builder edits).
    fn source_types(&self) -> &'static [&'static str] {
        &[]
    }

    /// The containers (channels/chats/threads) this connector is ingesting in
    /// `graph_id`, for the operational-state block. DEFAULT: derive distinct
    /// channels from `atoms` over [`source_types`](Self::source_types). Override
    /// ONLY when containers live outside atoms (e.g. watched-but-not-yet-ingested
    /// resources tracked in a side table).
    async fn describe_containers(
        &self,
        pool: &sqlx::PgPool,
        account_id: Uuid,
        graph_id: Uuid,
    ) -> Vec<state::Container> {
        state::containers_from_atoms(pool, account_id, graph_id, self.source_types()).await
    }

    /// Whether this provider supports pull-based "sync now" ingestion. Default
    /// `false` (e.g. Slack ingests via webhook, not pull).
    ///
    /// Note: this covers *non-sensitive, self-owned* data the user implicitly
    /// consents to by connecting (e.g. their own calendar). Sensitive
    /// correspondence (email) is **never** bulk-pulled — it enters only via
    /// [`fetch_items`](Self::fetch_items) (explicit per-thread opt-in, CS-116)
    /// and, for those opted-in threads, auto-capture of later replies scoped by
    /// conversation id (CS-117 / ADR-0008).
    fn supports_sync(&self) -> bool {
        false
    }

    /// Pull recent *self-owned, non-sensitive* items (e.g. the user's own
    /// calendar) and normalize them for ingestion. Must NOT bulk-read a user's
    /// inbox or other correspondence — those go through [`fetch_items`]. Providers
    /// may refresh the access token mid-run via `creds.refreshed`. Default:
    /// unsupported.
    ///
    /// `pool`/`account_id` (gsuite Train G3 PR 1): added so a provider that
    /// needs durable cursor state mid-sync (Google Calendar's incremental
    /// `syncToken` + resumable backfill page cursor — contract §6 test #6,
    /// "no silent truncation") can read/persist it without a second trait
    /// method. Same reasoning [`after_ingest`](Self::after_ingest) already
    /// carries `pool`/`account_id` for its own side effects. Implementers
    /// that don't need it (Microsoft's calendar-only `sync`) simply ignore
    /// both.
    async fn sync(
        &self,
        _http: &reqwest::Client,
        _pool: &sqlx::PgPool,
        _account_id: Uuid,
        _creds: &mut ConnectorCredentials,
    ) -> Result<Vec<NormalizedItem>, ConnectorError> {
        Err(ConnectorError::NotImplemented("sync not supported".into()))
    }

    /// Whether this provider supports explicit, opt-in ingestion of specific
    /// items by id (the per-thread "Ingest this thread" model — CS-116). Default
    /// `false`.
    fn supports_fetch_items(&self) -> bool {
        false
    }

    /// Fetch a specific set of items the user explicitly chose to ingest (e.g. an
    /// email thread they clicked "Ingest" on), by provider-native id, and
    /// normalize them. This is how sensitive correspondence enters the system —
    /// only what the user allowed, never the whole mailbox. Providers may refresh
    /// the access token via `creds.refreshed`. Default: unsupported.
    ///
    /// `scope` (gmail-options backend train, PR 1 of 4): [`IngestScope::
    /// Thread`] (the default — see that type's own doc) fetches every item in
    /// `item_ids`' thread(s)/conversation(s), today's only behavior.
    /// [`IngestScope::Message`] fetches only the named item id(s) — no
    /// thread/conversation expansion call at all. Implementers that don't
    /// support item ingestion (this default) ignore it, same as they already
    /// ignore `_creds`/`_item_ids`.
    async fn fetch_items(
        &self,
        _http: &reqwest::Client,
        _creds: &mut ConnectorCredentials,
        _item_ids: &[String],
        _scope: IngestScope,
    ) -> Result<Vec<NormalizedItem>, ConnectorError> {
        Err(ConnectorError::NotImplemented(
            "explicit item ingestion not supported".into(),
        ))
    }

    /// The `(oauth_tokens.provider, TokenOwner)` pair `routes::connectors::
    /// ingest_selected` should load credentials under (and persist any
    /// mid-run refresh back to) for THIS connector's [`fetch_items`](Self::
    /// fetch_items)/[`after_ingest`](Self::after_ingest) call (gsuite Train
    /// G2 PR 3). Default: `(self.id(), TokenOwner::AccountWide)` — every
    /// connector before Gmail, and Google's OWN `sync()` path (Calendar,
    /// still AccountWide, unaffected — this method is deliberately NOT
    /// consulted by `trigger_sync`).
    ///
    /// Exists because a per-surface scope split (Google — see
    /// `connectors::google::Surface::token_provider`/`token_owner_for`)
    /// means the SAME registered connector (`id() == "google"`) must load a
    /// DIFFERENT `oauth_tokens` row depending on which trait method is being
    /// served: Calendar's `sync()` reads the grandfathered `"google"`/
    /// `AccountWide` row, but Gmail's `fetch_items`/`after_ingest` must read
    /// the per-member `"google_gmail"`/`Member(user_id)` row instead — design
    /// doc `.scratch/gsuite-enterprise/g2-design.md` PR G2.3 item 3: "Token
    /// loads for Gmail use `("google_gmail", TokenOwner::Member(...))` —
    /// never AccountWide, no fallback." `connector.id()` alone cannot
    /// discriminate this (it is always `"google"`), so it has to be an
    /// explicit method a provider can override.
    fn fetch_items_credentials(&self, _user_id: Uuid) -> (&'static str, TokenOwner) {
        (self.id(), TokenOwner::AccountWide)
    }

    /// Provider-specific side effect run *after* a successful explicit ingest
    /// (`fetch_items` → atoms written). Best-effort: errors are logged by the
    /// caller, never surfaced to the user, so they can't fail the ingest the
    /// user already saw succeed.
    ///
    /// Microsoft uses this to register the just-ingested conversations for
    /// auto-capture of future replies — recording their `conversationId`s and
    /// ensuring a live Graph change-notification subscription exists (CS-117).
    /// Default: no-op.
    ///
    /// `owner` (m365 enterprise train PR 3.3): the [`TokenOwner`] `creds` were
    /// actually loaded/persisted under — `routes::connectors::finish_ingest`'s
    /// own `owner` parameter, unmodified. Microsoft's override uses it to
    /// record which member's mailbox a live subscription belongs to
    /// (`graph_subscriptions.owner_user_id`, migration `0190`/`0068`); every
    /// other connector ignores it, same as this default no-op.
    ///
    /// `control_pool` (gsuite Train G2 PR 7): `state.control_db()`, resolved
    /// once by `finish_ingest` — same precedent as `sync()`'s own
    /// `pool`/`account_id` addition (gsuite Train G3 PR 1): a provider that
    /// needs CONTROL-plane persistence for its own side effect (Gmail's watch
    /// self-heal — `oauth_tokens`-adjacent `google_watch_channels` is
    /// control-plane-only, same class as `oauth_tokens` itself) can now reach
    /// it without a second trait method. `None` only when the deploy has no
    /// control pool configured at all (`pool` above is still CONTENT-plane
    /// either way — the org's tenant pool under Org, unchanged). Implementers
    /// that don't need it (Microsoft, Slack) simply ignore it, same as they
    /// already ignore `_owner`/`_pool` when unused.
    ///
    /// `follow` (gmail-options backend train, PR 1 of 4): the wire-level
    /// `follow` knob on `routes::connectors::IngestSelectedRequest`
    /// (default `true` — byte-identical to every pre-PR call site). Gmail's
    /// and Microsoft's overrides write it onto the watched-thread row's
    /// `follow_replies` column (upserted regardless of its value — see the
    /// locked decision on `routes::connectors::ingest_selected`'s own doc).
    /// Implementers that don't watch anything (Slack, this default no-op)
    /// ignore it, same as they already ignore `_owner`/`_pool`.
    /// `follow_since` is present only for message-scope saves and captures
    /// request start before fetching. Providers persist it with the watched
    /// row so recovery cannot import pre-save history; None preserves legacy
    /// whole-thread behavior. It is independent of the follow boolean.
    #[allow(clippy::too_many_arguments)]
    async fn after_ingest(
        &self,
        _http: &reqwest::Client,
        _pool: &sqlx::PgPool,
        _account_id: Uuid,
        _user_id: Uuid,
        _graph_id: Uuid,
        _owner: TokenOwner,
        _creds: &ConnectorCredentials,
        _items: &[NormalizedItem],
        _control_pool: Option<&sqlx::PgPool>,
        _follow: bool,
        _follow_since: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stale-scope detection (CS-431, org-onboarding-from-the-invite.md §3 item
// 34) — shared by `slack::slack_missing_scopes` and
// `google::chat_missing_scopes`, which each add their own provider-specific
// "unknown" pre-check (Slack's backfill placeholder; see either call site's
// own doc comment) before calling this.
// ---------------------------------------------------------------------------

/// Which of `required`'s scopes are absent from `stored_scope` — a
/// space-separated OAuth grant string, e.g. `oauth_tokens.scope`.
///
/// Returns `None`, never a guessed empty `Vec`, when `stored_scope` is blank
/// (empty or all whitespace): a blank stored scope means "we don't actually
/// know what was granted", not "nothing was granted". This is reachable
/// beyond Slack's own legacy-backfill placeholder — a Google token refresh
/// response that omits `scope` (which Google's refresh grant does not
/// guarantee to include) would persist an empty string here if it were ever
/// re-stored, so the blank case is handled generically rather than only for
/// Slack's named placeholder. The "never guess" contract this function
/// exists to serve would be violated by treating that blank the same as "the
/// caller granted zero scopes".
///
/// `Some(vec)` (including `Some(vec![])`, meaning nothing is missing) is
/// returned for every other input, in `required`'s own order.
pub(crate) fn missing_scopes(stored_scope: &str, required: &[&str]) -> Option<Vec<String>> {
    if stored_scope.trim().is_empty() {
        return None;
    }
    let granted: std::collections::HashSet<&str> = stored_scope.split_whitespace().collect();
    Some(
        required
            .iter()
            .filter(|s| !granted.contains(*s))
            .map(|s| (*s).to_string())
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Registry — match-factory over `&'static` zero-sized connectors
// ---------------------------------------------------------------------------

static SLACK: SlackConnector = SlackConnector;
static MICROSOFT: MicrosoftConnector = MicrosoftConnector;
static GOOGLE: GoogleConnector = GoogleConnector;
static GITHUB: GithubConnector = GithubConnector;

/// Iterable registry — every registered connector, in display order. The
/// operational-state builder walks this so a newly-registered connector appears
/// in STATE for free (it just needs to declare its [`ConnectorProvider::source_types`]).
/// Keep in sync with [`lookup`].
pub(crate) static ALL: &[&'static dyn ConnectorProvider] = &[&SLACK, &MICROSOFT, &GOOGLE, &GITHUB];

/// Resolve a provider id to its connector. `None` → unknown provider (`404`).
///
/// A *registered* provider that isn't built yet (like `google`) is returned here
/// and signals via [`ConnectorError::NotImplemented`] (`501`) — that's what keeps
/// "not yet built" (`501`) distinct from "no such provider" (`404`).
pub fn lookup(provider: &str) -> Option<&'static dyn ConnectorProvider> {
    match provider {
        "slack" => Some(&SLACK),
        "microsoft" => Some(&MICROSOFT),
        "google" => Some(&GOOGLE),
        "github" => Some(&GITHUB),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_known_providers() {
        assert_eq!(lookup("slack").unwrap().id(), "slack");
        assert_eq!(lookup("microsoft").unwrap().id(), "microsoft");
        assert_eq!(lookup("google").unwrap().id(), "google");
        assert_eq!(lookup("github").unwrap().id(), "github");
    }

    #[test]
    fn lookup_unknown_provider_is_none() {
        assert!(lookup("bitbucket").is_none());
        assert!(lookup("").is_none());
    }

    #[test]
    fn github_is_registered_in_all() {
        assert!(ALL.iter().any(|c| c.id() == "github"));
    }

    #[test]
    fn github_source_types_include_all_six() {
        let types = lookup("github").unwrap().source_types();
        for t in [
            "github_pr",
            "github_issue",
            "github_release",
            "github_commit",
            "github_code",
            "github_code_summary",
        ] {
            assert!(types.contains(&t), "missing source_type {t}");
        }
    }

    #[test]
    fn slack_does_not_support_sync_microsoft_does() {
        assert!(!lookup("slack").unwrap().supports_sync());
        assert!(lookup("microsoft").unwrap().supports_sync());
    }

    #[test]
    fn slack_has_no_admin_consent_microsoft_does() {
        // Slack has no org-wide admin-consent flow; Microsoft does.
        assert!(lookup("slack")
            .unwrap()
            .admin_consent_url("http://x", "s")
            .is_none());
        assert!(lookup("microsoft")
            .unwrap()
            .admin_consent_url("http://x", "s")
            .is_some());
    }

    // ── missing_scopes (CS-431) ────────────────────────────────────────────

    #[test]
    fn missing_scopes_lists_what_the_grant_lacks() {
        let missing = missing_scopes("channels:read users:read", &["channels:read", "users:read", "users:read.email"])
            .expect("a non-blank grant is never unknown");
        assert_eq!(missing, vec!["users:read.email".to_string()]);
    }

    #[test]
    fn missing_scopes_empty_when_everything_is_granted() {
        let missing = missing_scopes("a b c", &["a", "b"]).expect("known grant");
        assert!(missing.is_empty(), "{missing:?}");
    }

    #[test]
    fn missing_scopes_none_for_a_blank_grant() {
        assert_eq!(missing_scopes("", &["a"]), None, "empty string is unknown, not '(all missing)'");
        assert_eq!(missing_scopes("   ", &["a"]), None, "whitespace-only is unknown too");
    }

    #[test]
    fn token_owner_account_wide_has_no_owner_id() {
        assert_eq!(TokenOwner::AccountWide.owner_user_id(), None);
    }

    #[test]
    fn token_owner_member_carries_its_uuid() {
        let id = Uuid::new_v4();
        assert_eq!(TokenOwner::Member(id).owner_user_id(), Some(id));
    }

    #[test]
    fn token_owner_variants_are_not_equal() {
        // Guards against a future refactor collapsing the two variants (e.g.
        // via a `From<Option<Uuid>>` shortcut) — AccountWide and a Member
        // with a nil uuid must never compare equal, since they select
        // different partial indexes.
        assert_ne!(TokenOwner::AccountWide, TokenOwner::Member(Uuid::nil()));
    }

    #[test]
    fn google_is_registered_and_implemented() {
        // Google is implemented (cs-uxp4a-calendar): registered (Some) and its
        // authorize_url never returns NotImplemented — it either succeeds (client
        // id configured) or reports NotConfigured. Asserting the absence of
        // NotImplemented is env-independent, so this test mutates no process-global
        // state and can't race the env-var reads in other tests.
        let g = lookup("google").unwrap();
        assert_eq!(g.id(), "google");
        let r = g.authorize_url("http://x", "state");
        assert!(
            !matches!(r, Err(ConnectorError::NotImplemented(_))),
            "google must be implemented, got {r:?}"
        );
    }
}
