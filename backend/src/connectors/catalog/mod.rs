//! The generic connector catalog (connectors redesign, layer B1):
//! `docs/design/connectors-redesign/PLAN.md`.
//!
//! One look, one lifecycle and one API for every connector. A connector is one
//! provider sign-in with sources inside it; each connector has one
//! [`ConnectorAdapter`] that wraps the existing provider code and answers the
//! catalog's questions. The adapter sits on top of
//! [`ConnectorProvider`](super::ConnectorProvider); it does not replace it.
//!
//! # What B1, B2 and B3 ship
//!
//! B1 is the read side: `grant`, `source_runtime`, `sources` and the metadata.
//! B2 adds `connect`, `complete_connect` and `disconnect` on every adapter (see
//! [`connect`]). B3 adds `scope_items`, `set_scope`, `set_enabled` and
//! `set_paused` (see [`scope`] and [`pause`]); the pause gate they write is read
//! at every place a provider's data becomes atoms. The remaining write method,
//! `start_backfill` (B4, history), is declared on the trait with a default body
//! that returns [`AdapterError::NotImplemented`]. One trait, not a read trait and
//! a write trait: later layers override a method per adapter without reshaping
//! the trait, and a default that errors cannot be mistaken for a working no-op.
//!
//! # Where things live
//!
//! * [`status`]: the one pure derivation of connector and source status.
//! * [`scope`]: scope checks, paging, the stored mode and Slack's pick filter.
//! * [`pause`]: the pause gate every capture entry point asks, and its writers.
//! * [`registry`]: the ten adapters, in display order.
//! * [`summary`]: assembles the wire `ConnectorSummary` from the adapters.
//! * `facts`: one account-scoped read of the control-plane rows adapters derive
//!   from, so a request costs one transaction, not one per connector.
//! * [`connect`]: connect, complete and disconnect helpers and the callback's
//!   redirect to `/connectors/return`.
//! * `adapters`: one module per provider.
//!
//! The backend ships codes and params, never sentences: [`Problem`] carries a
//! stable code and the web writes the words (`web/lib/api.ts`, "backend ships no
//! sentences"). The word "composio" never appears in a payload.
//!
//! Status, stats, feed and topics are generic over
//! [`SourceSpec::atom_source_types`]; adapters never implement them.

pub mod activity;
pub(crate) mod adapters;
pub mod connect;
pub mod everything;
mod facts;
pub(crate) mod github_facts;
pub mod history;
pub mod pause;
pub mod registry;
pub mod scope;
pub mod status;
pub mod summary;

#[cfg(test)]
mod pause_pg_tests;
#[cfg(test)]
mod pg_tests;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;
use crate::AppState;

pub(crate) use facts::Facts;

// ---------------------------------------------------------------------------
// Static description of a source
// ---------------------------------------------------------------------------

/// What a source lets the member pick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeShape {
    /// A list of items: "Only what I pick" or "Everything I can see".
    Items { noun: &'static str },
    /// A list of items with no "Everything" mode (meetings).
    PickOnly { noun: &'static str },
    /// No items to list; the source is everything or nothing.
    EverythingOnly,
}

impl ScopeShape {
    /// The wire value of `scope_shape`.
    pub fn wire(self) -> &'static str {
        match self {
            Self::Items { .. } => "items",
            Self::PickOnly { .. } => "pick_only",
            Self::EverythingOnly => "everything_only",
        }
    }

    /// The plural noun the picker counts, when the source has items.
    pub fn noun(self) -> Option<&'static str> {
        match self {
            Self::Items { noun } | Self::PickOnly { noun } => Some(noun),
            Self::EverythingOnly => None,
        }
    }

    /// Whether the member has to choose a scope before capture is meaningful.
    pub fn needs_scope_choice(self) -> bool {
        !matches!(self, Self::EverythingOnly)
    }
}

/// Whether a source can import history, and for how far back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySupport {
    /// `max_days` caps only the member's own choice; automatic windows stay as
    /// they are.
    Backfill { max_days: u32 },
    /// The source cannot backfill. `reason` is a stable code the web turns into
    /// a sentence, not a sentence itself.
    Unsupported { reason: &'static str },
}

/// One source inside a connector, as static data.
#[derive(Debug, Clone, Copy)]
pub struct SourceSpec {
    pub key: &'static str,
    pub label: &'static str,
    /// The `atoms.source_type` values this source writes. The sparkline, "last
    /// captured", the feed and the topics are generic over these (B5 reads them;
    /// until then the registry tests do).
    #[allow(dead_code)]
    pub atom_source_types: &'static [&'static str],
    pub scope: ScopeShape,
    pub history: HistorySupport,
}

// ---------------------------------------------------------------------------
// Runtime state read by the adapters
// ---------------------------------------------------------------------------

/// "Only what I pick" or "Everything I can see".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeMode {
    Pick,
    Everything,
}

impl ScopeMode {
    /// Parse the `connector_sources.scope_mode` column.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pick" => Some(Self::Pick),
            "everything" => Some(Self::Everything),
            _ => None,
        }
    }
}

/// A problem parameter: a string or a whole number, nothing else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ParamValue {
    Str(String),
    Num(i64),
}

/// A problem as a code and its params. No sentences: the web writes them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct Problem {
    pub code: String,
    /// The source the problem belongs to, or `None` for the whole connector.
    pub source: Option<String>,
    #[schema(value_type = String, format = DateTime)]
    pub since: DateTime<Utc>,
    #[schema(value_type = std::collections::BTreeMap<String, String>)]
    pub params: BTreeMap<String, ParamValue>,
}

impl Problem {
    pub fn new(code: &str, since: DateTime<Utc>) -> Self {
        Self {
            code: code.to_string(),
            source: None,
            since,
            params: BTreeMap::new(),
        }
    }

    pub fn with_param(mut self, key: &str, value: ParamValue) -> Self {
        self.params.insert(key.to_string(), value);
        self
    }

    pub fn for_source(mut self, source: &str) -> Self {
        self.source = Some(source.to_string());
        self
    }
}

/// How the sign-in itself is doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantHealth {
    Healthy,
    /// The credential is dead; only the member signing in again fixes it.
    ReconnectNeeded(Problem),
    /// The sign-in works but something about it needs the member's eyes.
    NeedsAttention(Problem),
}

/// The member's sign-in with one provider.
#[derive(Debug, Clone)]
pub struct GrantView {
    /// A human label for the account (a workspace name, an email), when the
    /// provider tables hold one.
    pub account_label: Option<String>,
    pub connected_at: DateTime<Utc>,
    pub health: GrantHealth,
}

/// An import in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Importing {
    /// `None` is indeterminate: the provider reports no progress.
    pub percent: Option<u8>,
    pub window_days: u32,
}

/// The member's chosen scope for one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeState {
    pub mode: ScopeMode,
    /// How many items are picked, for "Only what I pick"; `None` for
    /// "Everything I can see".
    pub count: Option<u32>,
}

/// What one source is doing for this member right now.
#[derive(Debug, Clone, Default)]
pub struct SourceRuntime {
    pub enabled: bool,
    pub paused: bool,
    /// Capture ended on the provider's side (an uninstalled or suspended app).
    pub stopped: bool,
    pub scope: Option<ScopeState>,
    /// The member's history choice: `Some(0)` is "None", `None` is not chosen.
    pub history_days: Option<u32>,
    pub importing: Option<Importing>,
    pub problem: Option<Problem>,
    pub setup_completed: bool,
}

// ---------------------------------------------------------------------------
// Adapter context and errors
// ---------------------------------------------------------------------------

/// The caller and what was already read for them. Built once per request.
///
/// The read methods answer from `facts`. The write methods reach the provider
/// code through `state`, `tenant` and `user`, the same three things the
/// per-provider route handlers they wrap are given.
pub struct AdapterCx<'a> {
    pub facts: &'a Facts,
    pub account_id: Uuid,
    pub user_id: Uuid,
    pub state: &'a Arc<AppState>,
    pub tenant: &'a Tenant,
    pub user: &'a UserContext,
}

impl<'a> AdapterCx<'a> {
    pub fn new(
        facts: &'a Facts,
        state: &'a Arc<AppState>,
        tenant: &'a Tenant,
        user: &'a UserContext,
    ) -> Self {
        Self {
            facts,
            account_id: user.account_id,
            user_id: user.user_id,
            state,
            tenant,
            user,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// The write side of the trait, until the layer that owns it lands. The
    /// payload names the operation.
    #[error("{0} is not implemented for this connector yet")]
    NotImplemented(&'static str),
    #[error("database error: {0}")]
    Database(String),
    /// The deployment has no credentials for this provider (503).
    #[error("this connector is not configured")]
    NotConfigured,
    /// The member has not finished signing in at the provider (409).
    #[error("the sign-in is not finished")]
    SignInPending,
    /// The sign-in expired or was abandoned; connect again (410).
    #[error("the sign-in expired")]
    SignInExpired,
    /// The member has nothing to disconnect or complete (404).
    #[error("not connected")]
    NotConnected,
    /// The existing provider path refused. `code` is a stable machine value the
    /// web turns into a sentence; the status is the one that path answered.
    #[error("refused ({status}): {code}")]
    Refused { status: u16, code: &'static str },
}

impl From<sqlx::Error> for AdapterError {
    fn from(e: sqlx::Error) -> Self {
        Self::Database(e.to_string())
    }
}

impl From<crate::store::StoreError> for AdapterError {
    fn from(e: crate::store::StoreError) -> Self {
        Self::Database(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Write-side types (B2 fills in the connect ones, B3 and B4 the rest)
// ---------------------------------------------------------------------------

/// Where the provider sign-in tab hands back to. Signed into the OAuth state, so
/// the callback of a connect started here lands on `/connectors/return`; a state
/// without it (the older UI) keeps redirecting as before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnTo {
    Wizard,
    Onboarding,
}

impl ReturnTo {
    /// The `ret` claim and the request body value.
    pub fn wire(self) -> &'static str {
        match self {
            Self::Wizard => "wizard",
            Self::Onboarding => "onboarding",
        }
    }

    /// Exactly the two known values; anything else is not a return target.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "wizard" => Some(Self::Wizard),
            "onboarding" => Some(Self::Onboarding),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectStart {
    pub authorize_url: String,
    pub expires_at: DateTime<Utc>,
    /// Set for hosted flows that finish with `complete_connect`.
    pub pending_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ScopeItemsQuery {
    pub q: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct ScopeItem {
    pub id: String,
    pub label: String,
    pub meta: Option<String>,
    pub selected: bool,
}

#[derive(Debug, Clone)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub total: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct ScopeSelection {
    pub mode: ScopeMode,
    pub item_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// The adapter
// ---------------------------------------------------------------------------

/// One connector: a provider sign-in with sources inside it.
#[async_trait]
pub trait ConnectorAdapter: Send + Sync {
    /// The `{provider}` path segment: `slack`, `microsoft`, `jira`, ...
    fn key(&self) -> &'static str;

    fn display_name(&self) -> &'static str;

    /// The Beta badge. The only visible difference between connector kinds.
    fn beta(&self) -> bool {
        false
    }

    fn sources(&self) -> &'static [SourceSpec];

    /// Pure grant derivation used by the readonly stored-model worker.
    fn grant_facts(&self, facts: &Facts) -> Result<Option<GrantView>, AdapterError>;

    /// The member's sign-in, or `None` when they have not connected.
    async fn grant(&self, cx: &AdapterCx<'_>) -> Result<Option<GrantView>, AdapterError>;

    /// What one source is doing for this member.
    async fn source_runtime(
        &self,
        cx: &AdapterCx<'_>,
        s: &SourceSpec,
    ) -> Result<SourceRuntime, AdapterError>;

    // ---- connect and disconnect (B2) ----------------------------------------

    /// Start a sign-in. The returned URL is where the browser goes next.
    /// Browser-redirect providers return the existing `/v1/oauth/{provider}/
    /// authorize` URL with a freshly minted state that carries `ret`; hosted
    /// providers return the provider's own link and a `pending_id`.
    async fn connect(
        &self,
        cx: &AdapterCx<'_>,
        ret: ReturnTo,
    ) -> Result<ConnectStart, AdapterError>;

    /// Finish a hosted sign-in. Only hosted providers have one; the OAuth
    /// providers finish in their callback, so asking them is a client error.
    async fn complete_connect(
        &self,
        _cx: &AdapterCx<'_>,
        _pending_id: &str,
    ) -> Result<(), AdapterError> {
        Err(AdapterError::Refused {
            status: 400,
            code: "nothing_to_complete",
        })
    }

    /// Revoke the member's sign-in and stop capture, through the provider's
    /// existing disconnect path. Captured atoms stay.
    async fn disconnect(&self, cx: &AdapterCx<'_>) -> Result<(), AdapterError>;

    // ---- scope and pause (B3) and history (B4) -------------------------------

    async fn scope_items(
        &self,
        _cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        _q: &ScopeItemsQuery,
    ) -> Result<Page<ScopeItem>, AdapterError> {
        Err(AdapterError::NotImplemented("scope_items"))
    }

    async fn set_scope(
        &self,
        _cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        _sel: &ScopeSelection,
    ) -> Result<(), AdapterError> {
        Err(AdapterError::NotImplemented("set_scope"))
    }

    async fn set_enabled(
        &self,
        _cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        _on: bool,
    ) -> Result<(), AdapterError> {
        Err(AdapterError::NotImplemented("set_enabled"))
    }

    async fn set_paused(
        &self,
        _cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        _paused: bool,
    ) -> Result<(), AdapterError> {
        Err(AdapterError::NotImplemented("set_paused"))
    }

    #[allow(dead_code)]
    async fn start_backfill(
        &self,
        _cx: &AdapterCx<'_>,
        _s: &SourceSpec,
        _days: u32,
    ) -> Result<(), AdapterError> {
        Err(AdapterError::NotImplemented("start_backfill"))
    }
}
