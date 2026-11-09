//! `GET /v1/connectors/status`: one normalized health row per connected
//! connector instance, for the Home dashboard's Connectors card and side
//! panel. Contract: `docs/implementation/connector-status-api.md`; validation:
//! `docs/implementation/connector-status-validation.md`.
//!
//! An in-process module, not a service: the handler is
//! `routes::connector_status`, the data is read straight from the tables the
//! connectors already write. Nothing here writes.
//!
//! # Not `services::connector_health`
//!
//! `services::connector_health` is D16's ORG-level record ("the member whose
//! Google grant backed your directory has left"), admin-only, one row per
//! degraded org capability. This module is the per-CONNECTION view for the
//! caller. They share no state and no public surface.
//!
//! # Shape of the module
//!
//! 1. [`fetch_control_inputs`] / [`fetch_atom_stats`]: the only SQL. Reads
//!    `oauth_tokens`, the per-provider sync-state tables, Composio
//!    connections and `mcp_tool_calls` on the control plane, and a 30-day
//!    atom count per `source_type` on the caller's content plane.
//! 2. [`assemble`]: pure. Turns those inputs into rows.
//! 3. [`derive`]: pure. The state rule (see below).
//!
//! # State rule
//!
//! Evaluated in this order, first match wins:
//!
//! 1. **failing**: a hard failure: the credential is marked invalid
//!    (`oauth_tokens.credential_invalid_at`), the access token expired with
//!    no refresh token, required scopes are missing, or a Composio connection
//!    needs a reconnect or failed its self-check.
//! 2. **failing**: the provider's last sync/renewal errored and nothing
//!    succeeded after it (`last_error_at` later than that same sub-sync's own
//!    latest success; another sub-sync's success or an ingested atom does not
//!    clear it, or one healthy repository would hide a failing one).
//! 3. **stale**: no success within the provider's threshold, measured from
//!    the newest of the last sync, the last ingested atom and the connect
//!    time (a freshly connected source gets a full threshold of grace).
//! 4. **healthy** otherwise.
//!
//! Thresholds ([`STALE_POLLED`], [`STALE_PUSH`], [`STALE_MCP`]): polled
//! sources (Meet, Microsoft calendar / Outlook everything-mode, Gmail
//! everything-mode, Composio) are judged against 24 hours because a worker
//! visits them on a short schedule; push-based sources (Slack, GitHub,
//! Google Calendar / Chat watches, Teams) are judged by their last event
//! against 7 days; the MCP connection against 14 days. A connection with no
//! independent liveness signal (`slack_user`) can fail but never goes stale.
//!
//! `action`: `reconnect` for every failing row; `sync_now` for a stale row
//! whose provider has `POST /v1/connectors/{provider}/sync` (Google Calendar
//! and Microsoft); `null` otherwise (a healthy row, or a stale push-based
//! source that has nothing to trigger).

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use sqlx::Row;
use uuid::Uuid;

/// Env flag. 404 from the route while unset. Owned here (and not shared with
/// the other Home PRs) so a merge conflict is a one-line thing.
pub const FLAG: &str = "HOME_DASHBOARD_ENABLED";

/// `HOME_DASHBOARD_ENABLED` is `1`/`true`. Unset = off.
pub fn home_dashboard_enabled() -> bool {
    std::env::var(FLAG)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Window for `items_30d`.
pub const WINDOW_DAYS: i64 = 30;

/// Stale after this long without a success: sources a worker polls.
pub const STALE_POLLED: Duration = Duration::hours(24);
/// Stale after this long without an event: push/webhook sources.
pub const STALE_PUSH: Duration = Duration::days(7);
/// Stale after this long without an MCP tool call or agent session.
pub const STALE_MCP: Duration = Duration::days(14);
/// `provider` (and `instance_id`) of the one MCP row.
pub const MCP_PROVIDER: &str = "mcp";

/// Longest error text returned. Stored errors are provider text; the row only
/// ever carries the first line, bounded.
const MESSAGE_MAX_CHARS: usize = 200;

// ── Response ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Healthy,
    Stale,
    Failing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Re-run the provider's OAuth flow / connect flow.
    Reconnect,
    /// `POST /v1/connectors/{provider}/sync`.
    SyncNow,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorStatusRow {
    /// The `oauth_tokens.provider` literal (`slack`, `google_gmail`,
    /// `microsoft`, ...), the Composio toolkit slug (`linear`), or `mcp`.
    pub provider: String,
    /// Stable per instance: `{provider}:{workspace or -}:{account|member}`,
    /// the Composio connection id, or `mcp`.
    pub instance_id: String,
    pub display_name: String,
    /// `chat`, `email`, `calendar`, `meetings`, `docs`, `code`, `tickets`,
    /// `agents`, `other`.
    pub kind_label: String,
    pub state: State,
    /// Last successful sync or ingested event, whichever is newer.
    pub last_synced_at: Option<DateTime<Utc>>,
    pub last_error_at: Option<DateTime<Utc>>,
    /// Human-readable; `null` when healthy.
    pub message: Option<String>,
    /// Atoms ingested from this connector in the last 30 days.
    pub items_30d: i64,
    pub action: Option<Action>,
    /// `failing` because the credential itself is dead (revoked, expired with
    /// no refresh token, missing scopes, or a Composio connection that needs
    /// a reconnect), as opposed to a sync that errored. Internal: Home's
    /// attention row words it as a sign-in; not part of this route's body.
    #[serde(skip)]
    pub reauth: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub total: i64,
    pub healthy: i64,
    pub stale: i64,
    pub failing: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorStatusResponse {
    pub connectors: Vec<ConnectorStatusRow>,
    pub summary: Summary,
}

// ── Pure state derivation ─────────────────────────────────────────────────

/// An error no later success has cleared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unresolved {
    pub at: DateTime<Utc>,
    pub message: String,
    /// How many sub-syncs are in this state (GitHub repos, calendar +
    /// Outlook). More than one appends "(+N more)".
    pub count: usize,
}

/// Everything [`derive`] needs about one instance.
#[derive(Debug, Clone)]
pub struct Signals {
    pub connected_at: Option<DateTime<Utc>>,
    /// A definite dead credential or missing permission, already worded.
    pub hard_failure: Option<String>,
    pub unresolved_error: Option<Unresolved>,
    /// Newest success (sync, renewal or ingested atom).
    pub last_ok: Option<DateTime<Utc>>,
    /// `None`: never stale.
    pub stale_after: Option<Duration>,
    pub can_sync_now: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derived {
    pub state: State,
    pub message: Option<String>,
    pub action: Option<Action>,
    pub last_error_at: Option<DateTime<Utc>>,
}

/// The state rule from the module doc.
pub fn derive(s: &Signals, now: DateTime<Utc>) -> Derived {
    let last_error_at = s.unresolved_error.as_ref().map(|u| u.at);
    if let Some(msg) = &s.hard_failure {
        return Derived {
            state: State::Failing,
            message: Some(msg.clone()),
            action: Some(Action::Reconnect),
            last_error_at,
        };
    }
    if let Some(u) = &s.unresolved_error {
        let message = if u.count > 1 {
            format!("{} (+{} more)", u.message, u.count - 1)
        } else {
            u.message.clone()
        };
        return Derived {
            state: State::Failing,
            message: Some(message),
            action: Some(Action::Reconnect),
            last_error_at,
        };
    }
    if let Some(after) = s.stale_after {
        let baseline = [s.last_ok, s.connected_at].into_iter().flatten().max();
        if let Some(b) = baseline {
            if now - b > after {
                let message = match s.last_ok {
                    Some(t) => format!("No successful sync since {}", t.format("%-d %b")),
                    None => "Connected but nothing has synced yet".to_string(),
                };
                return Derived {
                    state: State::Stale,
                    message: Some(message),
                    action: s.can_sync_now.then_some(Action::SyncNow),
                    last_error_at: None,
                };
            }
        }
    }
    Derived {
        state: State::Healthy,
        message: None,
        action: None,
        last_error_at: None,
    }
}

/// Human wording for `oauth_tokens.credential_invalid_reason`
/// (`CredentialDeathReason::code`). Unknown codes get the generic sentence,
/// never the raw code.
pub fn credential_message(reason: Option<&str>) -> String {
    match reason {
        Some("ms_password_changed_or_admin_reset") => {
            "Microsoft sign-in was reset (password change or admin reset). Reconnect to resume."
        }
        Some("ms_account_disabled") => "The Microsoft account was disabled by an administrator.",
        Some("ms_account_deleted") => "The Microsoft account no longer exists in its directory.",
        Some("ms_consent_revoked") => {
            "Access was revoked in Microsoft. Reconnect to grant it again."
        }
        Some("ms_grant_expired") | Some("ms_refresh_token_inactive") => {
            "Microsoft access expired from inactivity. Reconnect to resume."
        }
        Some("slack_token_revoked") | Some("slack_tokens_revoked_event") => {
            "Access was revoked in Slack. Reconnect to resume."
        }
        Some("slack_invalid_auth") => {
            "Slack no longer accepts this connection. Reconnect to resume."
        }
        Some("slack_account_inactive") => "The Slack user or workspace was deactivated.",
        Some("google_invalid_grant") | Some("google_unauthorized_after_refresh") => {
            "Google access was revoked or expired. Reconnect to resume."
        }
        _ => "Access was revoked. Reconnect to resume.",
    }
    .to_string()
}

/// First line of a stored error, trimmed and bounded. Empty becomes a generic
/// sentence.
pub fn sanitize_error(raw: &str) -> String {
    let line = raw.lines().next().unwrap_or("").trim();
    if line.is_empty() {
        return "The last sync failed".to_string();
    }
    if line.chars().count() <= MESSAGE_MAX_CHARS {
        return line.to_string();
    }
    let mut cut: String = line.chars().take(MESSAGE_MAX_CHARS - 1).collect();
    cut.push('\u{2026}');
    cut
}

fn github_error_message(code: &str) -> &'static str {
    match code {
        "destination_unreachable" => "A repository's destination ocean is no longer reachable",
        _ => "A repository failed to sync",
    }
}

// ── Provider catalog ──────────────────────────────────────────────────────

struct Descriptor {
    label: &'static str,
    kind: &'static str,
    source_types: Vec<&'static str>,
    /// `POST /v1/connectors/{provider}/sync` exists for this provider.
    can_sync_now: bool,
}

fn descriptor(provider: &str) -> Descriptor {
    use crate::connectors::{google_calendar, google_chat, google_gmail, google_meet};
    let d = |label, kind, source_types: Vec<&'static str>, can_sync_now| Descriptor {
        label,
        kind,
        source_types,
        can_sync_now,
    };
    match provider {
        "slack" => d("Slack", "chat", vec!["slack"], false),
        // Shares the `slack` source type with the bot connection; its atoms
        // are counted on `slack`, not twice.
        "slack_user" => d("Slack (personal)", "chat", vec![], false),
        "google" => d(
            "Google Calendar",
            "calendar",
            vec![google_calendar::SOURCE_TYPE],
            true,
        ),
        "google_gmail" => d("Gmail", "email", vec![google_gmail::SOURCE_TYPE], false),
        "google_chat" => d("Google Chat", "chat", vec![google_chat::SOURCE_TYPE], false),
        "google_meet" => d(
            "Google Meet",
            "meetings",
            vec![google_meet::SOURCE_TYPE],
            false,
        ),
        "microsoft" => d(
            "Microsoft 365",
            "email",
            vec![
                "outlook",
                crate::connectors::microsoft::CALENDAR_SOURCE_TYPE,
                "teams_channel",
                "teams_chat",
                "teams_transcript",
            ],
            true,
        ),
        "github" => d(
            "GitHub",
            "code",
            vec![
                "github_pr",
                "github_issue",
                "github_release",
                "github_commit",
                "github_code",
                "github_code_summary",
            ],
            false,
        ),
        _ => d("Connector", "other", vec![], false),
    }
}

fn composio_kind(service: &str) -> &'static str {
    match service {
        "linear" | "jira" | "dart" => "tickets",
        "confluence" => "docs",
        "hubspot" => "crm",
        "zendesk" => "support",
        _ => "other",
    }
}

// ── Raw inputs ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OauthRow {
    pub provider: String,
    pub workspace_id: Option<String>,
    pub scope: String,
    pub account_wide: bool,
    pub connected_at: DateTime<Utc>,
    pub credential_invalid_at: Option<DateTime<Utc>>,
    pub credential_invalid_reason: Option<String>,
    /// Access token expired and there is no refresh token to renew it.
    pub needs_reauth: bool,
}

/// One sub-sync's last outcome (a watch channel, an everything-mode row, one
/// watched repository).
#[derive(Debug, Clone)]
pub struct Obs {
    pub synced_at: Option<DateTime<Utc>>,
    pub error_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    /// A worker polls this on a schedule (judged against [`STALE_POLLED`]).
    pub polled: bool,
}

#[derive(Debug, Clone)]
pub struct ComposioRow {
    pub connection_id: Uuid,
    pub service: String,
    pub status: String,
    pub self_check: String,
    pub self_check_reason: Option<String>,
    pub scope_state: Option<String>,
    pub error_code: Option<String>,
    pub connected_at: DateTime<Utc>,
    pub last_capture_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct ControlInputs {
    pub oauth: Vec<OauthRow>,
    /// Keyed by `oauth_tokens.provider`.
    pub obs: HashMap<String, Vec<Obs>>,
    pub composio: Vec<ComposioRow>,
    pub slack_workspace_names: HashMap<String, String>,
    pub last_mcp_call_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct AtomStats {
    /// `source_type -> (atoms in the window, newest ingest time)`, every
    /// member's atoms in the oceans the caller can read.
    pub by_source: HashMap<String, (i64, DateTime<Utc>)>,
    /// The same, restricted to atoms the caller ingested.
    pub mine: HashMap<String, (i64, DateTime<Utc>)>,
}

impl AtomStats {
    fn view(&self, mine: bool) -> &HashMap<String, (i64, DateTime<Utc>)> {
        if mine {
            &self.mine
        } else {
            &self.by_source
        }
    }

    /// Totals over `source_types`. `mine`: only atoms the caller ingested
    /// (member-owned connections); otherwise every atom in the readable oceans.
    fn sum(&self, source_types: &[&str], mine: bool) -> (i64, Option<DateTime<Utc>>) {
        let mut n = 0;
        let mut last: Option<DateTime<Utc>> = None;
        for st in source_types {
            if let Some((c, t)) = self.view(mine).get(*st) {
                n += c;
                last = last.max(Some(*t));
            }
        }
        (n, last)
    }

    fn sum_prefix(&self, prefix: &str, mine: bool) -> (i64, Option<DateTime<Utc>>) {
        let mut n = 0;
        let mut last: Option<DateTime<Utc>> = None;
        for (st, (c, t)) in self.view(mine) {
            if st.starts_with(prefix) {
                n += c;
                last = last.max(Some(*t));
            }
        }
        (n, last)
    }
}

/// Whether a connection belongs to one member, so its count and liveness use
/// only the atoms that member ingested (`atoms.ingested_by`). A shared or team
/// ocean holds teammates' atoms of the same `source_type`; counting them would
/// hide a stalled personal poller. Account-level installs (the Slack bot,
/// GitHub, Google Chat) stay account-wide.
fn member_owned(provider: &str, account_wide: bool) -> bool {
    !account_wide || matches!(provider, "google_gmail" | "google_meet" | "slack_user")
}

/// Which `oauth_tokens` providers the caller may see (org flags), mirroring
/// `GET /v1/oauth/connections`. `None` = everything (personal plane).
pub type Visible<'a> = Option<&'a dyn Fn(&str) -> bool>;

// ── Assembly (pure) ───────────────────────────────────────────────────────

/// Merge a provider's sub-sync observations: the newest success, and the
/// unresolved errors (an error is resolved only by a later success in the SAME
/// sub-sync).
fn merge_obs(obs: &[Obs]) -> (Option<DateTime<Utc>>, Option<Unresolved>) {
    let last_synced = obs.iter().filter_map(|o| o.synced_at).max();
    let mut unresolved: Vec<(DateTime<Utc>, String)> = Vec::new();
    for o in obs {
        let Some(err_at) = o.error_at else { continue };
        if o.synced_at.is_none_or(|t| t < err_at) {
            let msg = o
                .error
                .as_deref()
                .map(sanitize_error)
                .unwrap_or_else(|| "The last sync failed".into());
            unresolved.push((err_at, msg));
        }
    }
    unresolved.sort_by_key(|(at, _)| *at);
    let count = unresolved.len();
    let unresolved = unresolved
        .pop()
        .map(|(at, message)| Unresolved { at, message, count });
    (last_synced, unresolved)
}

fn hard_failure_for(row: &OauthRow) -> Option<String> {
    if row.credential_invalid_at.is_some() {
        return Some(credential_message(row.credential_invalid_reason.as_deref()));
    }
    if row.needs_reauth {
        return Some(
            "The saved login expired and cannot be renewed. Reconnect to resume.".to_string(),
        );
    }
    let (missing, _unknown) =
        crate::routes::oauth::scope_reconsent_status(&row.provider, &row.scope);
    if !missing.is_empty() {
        return Some(format!(
            "Needs additional permissions ({}). Reconnect to grant them.",
            missing.join(", ")
        ));
    }
    None
}

/// Turn the raw inputs into the response. Pure.
pub fn assemble(
    inputs: &ControlInputs,
    atoms: &AtomStats,
    visible: Visible<'_>,
    now: DateTime<Utc>,
) -> ConnectorStatusResponse {
    let mut rows: Vec<ConnectorStatusRow> = Vec::new();
    let mut seen_provider: HashMap<&str, usize> = HashMap::new();
    let mut per_provider_total: HashMap<&str, usize> = HashMap::new();
    for r in &inputs.oauth {
        *per_provider_total.entry(r.provider.as_str()).or_default() += 1;
    }

    for r in &inputs.oauth {
        if let Some(v) = visible {
            if !v(&r.provider) {
                continue;
            }
        }
        let nth = seen_provider.entry(r.provider.as_str()).or_default();
        let first_of_provider = *nth == 0;
        *nth += 1;

        let desc = descriptor(&r.provider);
        // Atoms carry a source type, not a connection: with several
        // connections of one provider, the first (the credential in use, per
        // `GET /v1/oauth/connections` ordering) takes the count. Liveness is
        // shared: a second workspace is not stale just because the count is
        // attributed to the first.
        let (provider_items, atom_last) = atoms.sum(
            &desc.source_types,
            member_owned(&r.provider, r.account_wide),
        );
        let items = if first_of_provider { provider_items } else { 0 };
        let empty = Vec::new();
        let obs = inputs.obs.get(&r.provider).unwrap_or(&empty);
        let (obs_synced, unresolved) = merge_obs(obs);
        let last_ok = [obs_synced, atom_last].into_iter().flatten().max();

        let polled = obs.iter().any(|o| o.polled);
        let stale_after = match r.provider.as_str() {
            "slack_user" => None,
            _ if polled => Some(STALE_POLLED),
            _ => Some(STALE_PUSH),
        };

        let hard_failure = hard_failure_for(r);
        let reauth = hard_failure.is_some();
        let signals = Signals {
            connected_at: Some(r.connected_at),
            hard_failure,
            unresolved_error: unresolved,
            last_ok,
            stale_after,
            can_sync_now: desc.can_sync_now,
        };
        let d = derive(&signals, now);

        // A Slack workspace is named when its integration row says so; any
        // provider with several connections and no name falls back to the
        // workspace id so the panel never shows two identical rows.
        let slack_name = (r.provider == "slack")
            .then(|| {
                r.workspace_id
                    .as_ref()
                    .and_then(|w| inputs.slack_workspace_names.get(w))
            })
            .flatten();
        let several = per_provider_total
            .get(r.provider.as_str())
            .copied()
            .unwrap_or(0)
            > 1;
        let display_name = match (slack_name, &r.workspace_id) {
            (Some(name), _) => format!("{} \u{b7} {name}", desc.label),
            (None, Some(w)) if several && !w.is_empty() => format!("{} \u{b7} {w}", desc.label),
            _ => desc.label.to_string(),
        };

        rows.push(ConnectorStatusRow {
            provider: r.provider.clone(),
            instance_id: format!(
                "{}:{}:{}",
                r.provider,
                r.workspace_id.as_deref().unwrap_or("-"),
                if r.account_wide { "account" } else { "member" }
            ),
            display_name,
            kind_label: desc.kind.to_string(),
            state: d.state,
            last_synced_at: last_ok,
            last_error_at: d.last_error_at,
            message: d.message,
            items_30d: items,
            action: d.action,
            reauth,
        });
    }

    for c in &inputs.composio {
        let label = crate::connectors::composio::recipe::all()
            .find(|rc| rc.toolkit == c.service)
            .map(|rc| rc.label.clone())
            .unwrap_or_else(|| c.service.clone());
        let (items, atom_last) = atoms.sum_prefix(&format!("{}_", c.service), true);
        let reauth =
            c.status == "reconnect_needed" || c.scope_state.as_deref() == Some("reconnect_needed");
        let hard = if reauth {
            Some(format!(
                "{label} access was revoked or expired. Reconnect to resume capture."
            ))
        } else if c.status == "needs_attention"
            || c.scope_state.as_deref() == Some("needs_attention")
        {
            Some(match c.error_code.as_deref() {
                Some(code) if !code.is_empty() => {
                    format!("{label} capture needs attention ({})", sanitize_error(code))
                }
                _ => format!("{label} capture needs attention"),
            })
        } else if c.self_check == "failed" {
            Some(match c.self_check_reason.as_deref() {
                Some(r) if !r.trim().is_empty() => sanitize_error(r),
                _ => format!("{label} failed its read check"),
            })
        } else {
            None
        };
        let last_ok = [c.last_capture_at, atom_last].into_iter().flatten().max();
        let signals = Signals {
            connected_at: Some(c.connected_at),
            hard_failure: hard,
            unresolved_error: None,
            last_ok,
            stale_after: Some(STALE_POLLED),
            can_sync_now: false,
        };
        let d = derive(&signals, now);
        rows.push(ConnectorStatusRow {
            provider: c.service.clone(),
            instance_id: c.connection_id.to_string(),
            display_name: label,
            kind_label: composio_kind(&c.service).to_string(),
            state: d.state,
            last_synced_at: last_ok,
            last_error_at: d.last_error_at,
            message: d.message,
            items_30d: items,
            action: d.action,
            reauth,
        });
    }

    if let Some(last_call) = inputs.last_mcp_call_at {
        let (items, atom_last) = atoms.sum(&["coding_session"], true);
        let last_ok = [Some(last_call), atom_last].into_iter().flatten().max();
        let signals = Signals {
            connected_at: None,
            hard_failure: None,
            unresolved_error: None,
            last_ok,
            stale_after: Some(STALE_MCP),
            can_sync_now: false,
        };
        let d = derive(&signals, now);
        rows.push(ConnectorStatusRow {
            provider: MCP_PROVIDER.into(),
            instance_id: MCP_PROVIDER.into(),
            display_name: "MCP agents".into(),
            kind_label: "agents".into(),
            state: d.state,
            last_synced_at: last_ok,
            last_error_at: None,
            message: d.message,
            items_30d: items,
            action: None,
            reauth: false,
        });
    }

    sort_rows(&mut rows);
    let summary = summarize(&rows);
    ConnectorStatusResponse {
        connectors: rows,
        summary,
    }
}

/// Failing, then stale, then healthy; within a group, by 30-day volume
/// (descending), then name, then id so the order is stable.
pub fn sort_rows(rows: &mut [ConnectorStatusRow]) {
    let rank = |s: State| match s {
        State::Failing => 0,
        State::Stale => 1,
        State::Healthy => 2,
    };
    rows.sort_by(|a, b| {
        rank(a.state)
            .cmp(&rank(b.state))
            .then(b.items_30d.cmp(&a.items_30d))
            .then(a.display_name.cmp(&b.display_name))
            .then(a.instance_id.cmp(&b.instance_id))
    });
}

pub fn summarize(rows: &[ConnectorStatusRow]) -> Summary {
    let mut s = Summary {
        total: rows.len() as i64,
        ..Default::default()
    };
    for r in rows {
        match r.state {
            State::Healthy => s.healthy += 1,
            State::Stale => s.stale += 1,
            State::Failing => s.failing += 1,
        }
    }
    s
}

// ── SQL ───────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
}

/// Control-plane reads for `(account_id, user_id)`, inside the caller's
/// account-scoped read-only transaction (`begin_account_scoped` on the
/// control plane; the caller rolls it back). Row visibility matches `GET /v1/oauth/connections`:
/// the account-wide rows plus the caller's own member rows.
async fn fetch_control_inputs_inner(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    user_id: Uuid,
    include_control_github: bool,
) -> Result<ControlInputs, Error> {
    let mut out = ControlInputs::default();

    // Same ordering as `GET /v1/oauth/connections`: per provider the caller's
    // own row first, then newest, so "first of provider" is the credential in
    // use.
    let rows = sqlx::query(
        "SELECT provider, provider_workspace_id, scope, created_at, \
                credential_invalid_at, credential_invalid_reason, \
                (owner_user_id IS NULL) AS account_wide, \
                (expires_at IS NOT NULL AND expires_at < now() AND refresh_token IS NULL) AS needs_reauth \
         FROM oauth_tokens \
         WHERE account_id = $1 AND (owner_user_id IS NULL OR owner_user_id = $2) \
         ORDER BY provider, (owner_user_id IS NULL), updated_at DESC",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(&mut **tx)
    .await?;
    for r in rows {
        out.oauth.push(OauthRow {
            provider: r.get("provider"),
            workspace_id: r.get("provider_workspace_id"),
            scope: r.get("scope"),
            account_wide: r.get("account_wide"),
            connected_at: r.get("created_at"),
            credential_invalid_at: r.get("credential_invalid_at"),
            credential_invalid_reason: r.get("credential_invalid_reason"),
            needs_reauth: r.get("needs_reauth"),
        });
    }

    let mut push =
        |provider: &str, o: Obs| out.obs.entry(provider.to_string()).or_default().push(o);

    // Google push watches (Calendar -> `google`, Gmail, Chat). Gmail and Meet
    // watches are per member (`owner_user_id`, migration 0186): another
    // member's watch, and its error text (which can name their address), must
    // not reach this caller. NULL owner = an account-level watch.
    let rows = sqlx::query(
        "SELECT surface, COALESCE(last_renewed_at, created_at) AS ok_at, last_error, last_error_at \
         FROM google_watch_channels \
         WHERE account_id = $1 AND (owner_user_id IS NULL OR owner_user_id = $2)",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(&mut **tx)
    .await?;
    for r in rows {
        let provider = match r.get::<String, _>("surface").as_str() {
            "calendar" => "google",
            "gmail" => "google_gmail",
            "chat" => "google_chat",
            _ => continue,
        };
        push(
            provider,
            Obs {
                synced_at: r.get("ok_at"),
                error_at: r.get("last_error_at"),
                error: r.get("last_error"),
                polled: false,
            },
        );
    }

    // Per-member pollers: only rows that are actually being worked.
    for (table, provider, enabled_col) in [
        ("gmail_everything_mode", "google_gmail", "enabled"),
        ("outlook_everything_mode", "microsoft", "enabled"),
        ("microsoft_calendar_sync_state", "microsoft", "sync_enabled"),
        ("google_meet_sync_state", "google_meet", "TRUE"),
    ] {
        // Table and column names are the constants above, never input.
        let sql = format!(
            "SELECT last_synced_at, last_error, last_error_at FROM {table} \
             WHERE account_id = $1 AND owner_user_id = $2 AND {enabled_col}"
        );
        let rows = sqlx::query(&sql)
            .bind(account_id)
            .bind(user_id)
            .fetch_all(&mut **tx)
            .await?;
        for r in rows {
            push(
                provider,
                Obs {
                    synced_at: r.get("last_synced_at"),
                    error_at: r.get("last_error_at"),
                    error: r.get("last_error"),
                    polled: true,
                },
            );
        }
    }

    if include_control_github {
        // GitHub: every active watched repository is a sub-sync. A cleared
        // `sync_error_code` is the success signal.
        let rows = sqlx::query(
            "SELECT last_synced_at, sync_error_code, sync_error_at FROM github_watched_repos \
         WHERE account_id = $1 AND active",
        )
        .bind(account_id)
        .fetch_all(&mut **tx)
        .await?;
        for r in rows {
            let code: Option<String> = r.get("sync_error_code");
            let error_at: Option<DateTime<Utc>> = r.get("sync_error_at");
            push(
                "github",
                Obs {
                    synced_at: r.get("last_synced_at"),
                    // A present code is failing now (it is cleared by the next tick
                    // that makes progress), even if its timestamp is NULL.
                    error_at: code.as_ref().map(|_| error_at.unwrap_or_else(Utc::now)),
                    error: code.as_deref().map(|c| github_error_message(c).to_string()),
                    polled: false,
                },
            );
        }
    }
    let rows = sqlx::query(
        "SELECT workspace_id, workspace_name FROM slack_integrations WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;
    for r in rows {
        let name: String = r.get("workspace_name");
        if !name.trim().is_empty() {
            out.slack_workspace_names
                .insert(r.get("workspace_id"), name);
        }
    }

    // Composio: the caller's own live connections.
    let rows = sqlx::query(
        "SELECT c.id, c.service, c.status, c.self_check, c.self_check_reason, c.created_at, \
                s.state AS scope_state, s.error_code, s.last_capture_at \
         FROM composio_connections c \
         LEFT JOIN LATERAL ( \
             SELECT state, error_code, last_capture_at FROM composio_capture_scopes \
             WHERE connection_id = c.id AND $3 ORDER BY updated_at DESC LIMIT 1 \
         ) s ON TRUE \
         WHERE c.account_id = $1 AND c.owner_user_id = $2 \
           AND c.status NOT IN ('pending', 'disconnected') \
         ORDER BY c.created_at",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(include_control_github)
    .fetch_all(&mut **tx)
    .await?;
    for r in rows {
        out.composio.push(ComposioRow {
            connection_id: r.get("id"),
            service: r.get("service"),
            status: r.get("status"),
            self_check: r.get("self_check"),
            self_check_reason: r.get("self_check_reason"),
            scope_state: r.get("scope_state"),
            error_code: r.get("error_code"),
            connected_at: r.get("created_at"),
            last_capture_at: r.get("last_capture_at"),
        });
    }

    out.last_mcp_call_at = sqlx::query_scalar(
        "SELECT MAX(created_at) FROM mcp_tool_calls WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(out)
}

pub async fn fetch_control_inputs(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<ControlInputs, Error> {
    fetch_control_inputs_inner(tx, account_id, user_id, true).await
}
/// Worker control facts exclude tenant GitHub metadata, which is separately
/// read on the verified content transaction and explicit graph set.
pub async fn fetch_control_inputs_scoped(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<ControlInputs, Error> {
    fetch_control_inputs_inner(tx, account_id, user_id, false).await
}
/// Atoms ingested in the last [`WINDOW_DAYS`] per `source_type`, restricted to
/// the oceans the caller may read (`readable_graph_ids`, the same read scope
/// as every other account-wide listing). Runs inside the caller's own
/// content-plane transaction.
pub async fn fetch_atom_stats(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    user_id: Uuid,
    readable_graph_ids: &[Uuid],
    now: DateTime<Utc>,
) -> Result<AtomStats, Error> {
    if readable_graph_ids.is_empty() {
        return Ok(AtomStats::default());
    }
    // The team-seat history floor applies like on every member read of atoms
    // (`crate::history_floor`): a late joiner's counts start at their floor.
    let sql = format!(
        "SELECT a.source_type, COUNT(*) AS n, MAX(a.created_at) AS last_at, \
                COUNT(*) FILTER (WHERE a.ingested_by = $4) AS my_n, \
                MAX(a.created_at) FILTER (WHERE a.ingested_by = $4) AS my_last_at \
         FROM graphs g CROSS JOIN LATERAL library_permitted_atoms(g.id,NULL) p JOIN atoms a ON a.graph_id=g.id AND a.atom_id=p.atom_id \
         WHERE g.account_id = $1 AND g.id = ANY($2::uuid[]) AND a.created_at >= $3 \
           AND {floor} \
         GROUP BY a.source_type",
        floor = crate::history_floor::admits_atom("a"),
    );
    let rows = sqlx::query(&sql)
        .bind(account_id)
        .bind(readable_graph_ids)
        .bind(now - Duration::days(WINDOW_DAYS))
        .bind(user_id)
        .fetch_all(&mut **tx)
        .await?;
    let mut stats = AtomStats::default();
    for r in rows {
        let source_type: String = r.get("source_type");
        let my_n: i64 = r.get("my_n");
        if my_n > 0 {
            if let Some(last) = r.get::<Option<DateTime<Utc>>, _>("my_last_at") {
                stats.mine.insert(source_type.clone(), (my_n, last));
            }
        }
        stats
            .by_source
            .insert(source_type, (r.get("n"), r.get("last_at")));
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, d, h, 0, 0).unwrap()
    }
    const NOW_D: u32 = 29;

    fn sig() -> Signals {
        Signals {
            connected_at: Some(t(1, 0)),
            hard_failure: None,
            unresolved_error: None,
            last_ok: Some(t(29, 6)),
            stale_after: Some(STALE_POLLED),
            can_sync_now: true,
        }
    }

    #[test]
    fn healthy_has_no_message_or_action() {
        let d = derive(&sig(), t(NOW_D, 12));
        assert_eq!(d.state, State::Healthy);
        assert_eq!(d.message, None);
        assert_eq!(d.action, None);
    }

    #[test]
    fn hard_failure_wins_over_everything_and_reconnects() {
        let mut s = sig();
        s.hard_failure = Some("revoked".into());
        s.last_ok = None;
        let d = derive(&s, t(NOW_D, 12));
        assert_eq!(
            (d.state, d.action),
            (State::Failing, Some(Action::Reconnect))
        );
        assert_eq!(d.message.as_deref(), Some("revoked"));
    }

    #[test]
    fn unresolved_error_fails_and_counts_extras() {
        let mut s = sig();
        s.unresolved_error = Some(Unresolved {
            at: t(29, 8),
            message: "boom".into(),
            count: 3,
        });
        let d = derive(&s, t(NOW_D, 12));
        assert_eq!(d.state, State::Failing);
        assert_eq!(d.message.as_deref(), Some("boom (+2 more)"));
        assert_eq!(d.last_error_at, Some(t(29, 8)));
    }

    #[test]
    fn stale_past_threshold_offers_sync_now_only_when_available() {
        let mut s = sig();
        s.last_ok = Some(t(27, 0));
        let d = derive(&s, t(NOW_D, 12));
        assert_eq!((d.state, d.action), (State::Stale, Some(Action::SyncNow)));
        assert_eq!(
            d.message.as_deref(),
            Some("No successful sync since 27 Oct")
        );
        s.can_sync_now = false;
        assert_eq!(derive(&s, t(NOW_D, 12)).action, None);
    }

    #[test]
    fn threshold_is_exclusive() {
        let mut s = sig();
        s.last_ok = Some(t(28, 12));
        assert_eq!(derive(&s, t(NOW_D, 12)).state, State::Healthy);
        assert_eq!(derive(&s, t(NOW_D, 13)).state, State::Stale);
    }

    #[test]
    fn fresh_connection_gets_a_full_threshold_of_grace() {
        let mut s = sig();
        s.last_ok = None;
        s.connected_at = Some(t(29, 0));
        assert_eq!(derive(&s, t(NOW_D, 12)).state, State::Healthy);
        s.connected_at = Some(t(20, 0));
        let d = derive(&s, t(NOW_D, 12));
        assert_eq!(d.state, State::Stale);
        assert_eq!(
            d.message.as_deref(),
            Some("Connected but nothing has synced yet")
        );
    }

    #[test]
    fn no_threshold_means_never_stale() {
        let mut s = sig();
        s.stale_after = None;
        s.last_ok = None;
        s.connected_at = Some(t(1, 0));
        assert_eq!(derive(&s, t(NOW_D, 12)).state, State::Healthy);
    }

    #[test]
    fn merge_obs_resolves_only_by_the_same_subsyncs_later_success() {
        let o = |synced, err| Obs {
            synced_at: synced,
            error_at: err,
            error: Some("bad".into()),
            polled: true,
        };
        // error after the sub-sync's own success: unresolved
        let (_, u) = merge_obs(&[o(Some(t(28, 0)), Some(t(29, 0)))]);
        assert!(u.is_some());
        // success after the error: resolved
        let (_, u) = merge_obs(&[o(Some(t(29, 1)), Some(t(29, 0)))]);
        assert!(u.is_none());
        // another sub-sync's success does not clear it
        let (_, u) = merge_obs(&[o(Some(t(29, 5)), None), o(Some(t(28, 0)), Some(t(29, 0)))]);
        assert!(u.is_some());
        // never synced, errored: unresolved
        let (_, u) = merge_obs(&[o(None, Some(t(29, 0)))]);
        assert!(u.is_some());
    }

    #[test]
    fn merge_obs_counts_failing_subsyncs_and_keeps_the_newest_message() {
        let o = |err_at, msg: &str| Obs {
            synced_at: None,
            error_at: Some(err_at),
            error: Some(msg.into()),
            polled: false,
        };
        let (_, u) = merge_obs(&[o(t(28, 0), "old"), o(t(29, 0), "new")]);
        let u = u.unwrap();
        assert_eq!((u.count, u.message.as_str(), u.at), (2, "new", t(29, 0)));
    }

    #[test]
    fn credential_messages_never_echo_the_raw_code() {
        assert!(credential_message(Some("slack_token_revoked")).contains("Slack"));
        let unknown = credential_message(Some("totally_new_code"));
        assert!(!unknown.contains("totally_new_code"));
        assert!(credential_message(None).contains("Reconnect"));
    }

    #[test]
    fn sanitize_error_takes_one_bounded_line() {
        assert_eq!(sanitize_error("first\nsecond"), "first");
        assert_eq!(sanitize_error("  \n"), "The last sync failed");
        let long = "x".repeat(500);
        let out = sanitize_error(&long);
        assert_eq!(out.chars().count(), MESSAGE_MAX_CHARS);
        assert!(out.ends_with('\u{2026}'));
    }

    fn oauth(provider: &str, ws: Option<&str>) -> OauthRow {
        OauthRow {
            provider: provider.into(),
            workspace_id: ws.map(String::from),
            scope: String::new(),
            account_wide: true,
            connected_at: t(1, 0),
            credential_invalid_at: None,
            credential_invalid_reason: None,
            needs_reauth: false,
        }
    }

    #[test]
    fn assemble_sorts_failing_then_stale_then_healthy_by_volume_and_summarizes() {
        let mut revoked = oauth("slack", Some("T1"));
        revoked.credential_invalid_at = Some(t(26, 0));
        revoked.credential_invalid_reason = Some("slack_token_revoked".into());
        let mut google_calendar = oauth("google", None);
        google_calendar.connected_at = t(29, 0);
        let inputs = ControlInputs {
            oauth: vec![
                oauth("github", Some("g")),
                revoked,
                google_calendar,
                oauth("google_meet", None),
            ],
            obs: HashMap::from([(
                "google_meet".to_string(),
                vec![Obs {
                    synced_at: Some(t(29, 10)),
                    error_at: None,
                    error: None,
                    polled: true,
                }],
            )]),
            ..Default::default()
        };
        let mut atoms = AtomStats::default();
        atoms.by_source.insert("github_pr".into(), (40, t(29, 9)));
        atoms.by_source.insert("meet".into(), (7, t(29, 10)));
        atoms.mine.insert("meet".into(), (7, t(29, 10)));
        atoms.by_source.insert("slack".into(), (999, t(29, 10)));
        let out = assemble(&inputs, &atoms, None, t(NOW_D, 12));
        let order: Vec<_> = out
            .connectors
            .iter()
            .map(|r| (r.provider.as_str(), r.state))
            .collect();
        assert_eq!(
            order,
            vec![
                ("slack", State::Failing),
                ("github", State::Healthy),
                ("google_meet", State::Healthy),
                ("google", State::Healthy),
            ]
        );
        assert_eq!(
            out.summary,
            Summary {
                total: 4,
                healthy: 3,
                stale: 0,
                failing: 1
            }
        );
        assert_eq!(out.connectors[0].action, Some(Action::Reconnect));
        assert_eq!(out.connectors[0].items_30d, 999);
        assert!(out.connectors[0]
            .message
            .as_deref()
            .unwrap()
            .contains("Slack"));
    }

    #[test]
    fn assemble_attributes_atoms_to_the_first_connection_of_a_provider_only() {
        let inputs = ControlInputs {
            oauth: vec![oauth("slack", Some("T1")), oauth("slack", Some("T2"))],
            ..Default::default()
        };
        let mut atoms = AtomStats::default();
        atoms.by_source.insert("slack".into(), (5, t(29, 10)));
        let out = assemble(&inputs, &atoms, None, t(NOW_D, 12));
        let counts: Vec<_> = out.connectors.iter().map(|r| r.items_30d).collect();
        assert_eq!(counts, vec![5, 0]);
        // Same display name would collide in the panel; the workspace id splits them.
        assert_ne!(
            out.connectors[0].display_name,
            out.connectors[1].display_name
        );
    }

    #[test]
    fn assemble_polled_provider_goes_stale_on_24h_and_push_on_7d() {
        let mut inputs = ControlInputs {
            oauth: vec![oauth("google_meet", None), oauth("google_chat", None)],
            ..Default::default()
        };
        inputs.obs.insert(
            "google_meet".into(),
            vec![Obs {
                synced_at: Some(t(27, 0)),
                error_at: None,
                error: None,
                polled: true,
            }],
        );
        inputs.obs.insert(
            "google_chat".into(),
            vec![Obs {
                synced_at: Some(t(27, 0)),
                error_at: None,
                error: None,
                polled: false,
            }],
        );
        let out = assemble(&inputs, &AtomStats::default(), None, t(NOW_D, 12));
        let by: HashMap<_, _> = out
            .connectors
            .iter()
            .map(|r| (r.provider.as_str(), r.state))
            .collect();
        assert_eq!(by["google_meet"], State::Stale);
        assert_eq!(by["google_chat"], State::Healthy);
    }

    #[test]
    fn member_owned_connection_ignores_teammates_atoms() {
        // A stalled Gmail poller: teammates' gmail atoms are fresh and large,
        // the caller has none in the window.
        let mut row = oauth("google_gmail", None);
        row.account_wide = false;
        let inputs = ControlInputs {
            oauth: vec![row],
            obs: HashMap::from([(
                "google_gmail".to_string(),
                vec![Obs {
                    synced_at: Some(t(25, 0)),
                    error_at: None,
                    error: None,
                    polled: true,
                }],
            )]),
            ..Default::default()
        };
        let mut atoms = AtomStats::default();
        atoms.by_source.insert("gmail".into(), (500, t(29, 11)));
        let out = assemble(&inputs, &atoms, None, t(NOW_D, 12));
        assert_eq!(out.connectors[0].state, State::Stale);
        assert_eq!(out.connectors[0].items_30d, 0);
        atoms.mine.insert("gmail".into(), (3, t(29, 11)));
        let out = assemble(&inputs, &atoms, None, t(NOW_D, 12));
        assert_eq!(out.connectors[0].state, State::Healthy);
        assert_eq!(out.connectors[0].items_30d, 3);
    }

    #[test]
    fn account_level_connection_counts_the_whole_account() {
        let inputs = ControlInputs {
            oauth: vec![oauth("slack", Some("T1"))],
            ..Default::default()
        };
        let mut atoms = AtomStats::default();
        atoms.by_source.insert("slack".into(), (40, t(29, 11)));
        let out = assemble(&inputs, &atoms, None, t(NOW_D, 12));
        assert_eq!(out.connectors[0].items_30d, 40);
    }

    #[test]
    fn assemble_google_calendar_stale_offers_sync_now() {
        let mut row = oauth("google", None);
        row.connected_at = t(1, 0);
        let inputs = ControlInputs {
            oauth: vec![row],
            ..Default::default()
        };
        let out = assemble(&inputs, &AtomStats::default(), None, t(NOW_D, 12));
        assert_eq!(out.connectors[0].state, State::Stale);
        assert_eq!(out.connectors[0].action, Some(Action::SyncNow));
    }

    #[test]
    fn assemble_visibility_filter_hides_disabled_providers() {
        let only_slack = |p: &str| p == "slack";
        let inputs = ControlInputs {
            oauth: vec![oauth("slack", None), oauth("github", None)],
            ..Default::default()
        };
        let out = assemble(
            &inputs,
            &AtomStats::default(),
            Some(&only_slack),
            t(NOW_D, 12),
        );
        assert_eq!(out.summary.total, 1);
        assert_eq!(out.connectors[0].provider, "slack");
    }

    #[test]
    fn assemble_mcp_row_only_with_a_call_and_never_fails() {
        let none = assemble(
            &ControlInputs::default(),
            &AtomStats::default(),
            None,
            t(NOW_D, 12),
        );
        assert_eq!(none.summary.total, 0);

        let mut inputs = ControlInputs {
            last_mcp_call_at: Some(t(1, 0)),
            ..Default::default()
        };
        let mut atoms = AtomStats::default();
        atoms.mine.insert("coding_session".into(), (12, t(10, 0)));
        let out = assemble(&inputs, &atoms, None, t(NOW_D, 12));
        assert_eq!(out.connectors[0].provider, "mcp");
        assert_eq!(out.connectors[0].items_30d, 12);
        assert_eq!(out.connectors[0].state, State::Stale);
        assert_eq!(out.connectors[0].action, None);
        inputs.last_mcp_call_at = Some(t(28, 0));
        let out = assemble(&inputs, &AtomStats::default(), None, t(NOW_D, 12));
        assert_eq!(out.connectors[0].state, State::Healthy);
    }

    #[test]
    fn assemble_composio_states() {
        let base = ComposioRow {
            connection_id: Uuid::nil(),
            service: "linear".into(),
            status: "active".into(),
            self_check: "passed".into(),
            self_check_reason: None,
            scope_state: Some("active".into()),
            error_code: None,
            connected_at: t(1, 0),
            last_capture_at: Some(t(29, 11)),
        };
        let mut reconnect = base.clone();
        reconnect.status = "reconnect_needed".into();
        let mut check_failed = base.clone();
        check_failed.self_check = "failed".into();
        check_failed.self_check_reason = Some("Missing read scope".into());
        let mut quiet = base.clone();
        quiet.last_capture_at = Some(t(20, 0));
        let inputs = ControlInputs {
            composio: vec![base, reconnect, check_failed, quiet],
            ..Default::default()
        };
        let out = assemble(&inputs, &AtomStats::default(), None, t(NOW_D, 12));
        assert_eq!(
            out.summary,
            Summary {
                total: 4,
                healthy: 1,
                stale: 1,
                failing: 2
            }
        );
        assert!(out.connectors.iter().all(|r| r.kind_label == "tickets"));
        assert!(out
            .connectors
            .iter()
            .any(|r| r.message.as_deref() == Some("Missing read scope")));
    }
}

// Live-Postgres proof of the SQL half: every source table is read with the
// right ownership and the assembled states come out as the contract says.
//
//   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib services::connector_status::connector_status_pg_tests
#[cfg(test)]
mod connector_status_pg_tests {
    use super::*;
    use sqlx::postgres::PgConnectOptions;
    use std::str::FromStr;

    async fn user_account(pool: &sqlx::Pool<sqlx::Postgres>, email: &str) -> (Uuid, Uuid) {
        let user: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap();
        let account: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, plan_tier) VALUES ($1, 'free') RETURNING id",
        )
        .bind(user)
        .fetch_one(pool)
        .await
        .unwrap();
        (user, account)
    }

    async fn token(
        pool: &sqlx::Pool<sqlx::Postgres>,
        account: Uuid,
        owner: Option<Uuid>,
        provider: &str,
        ws: &str,
        invalid_reason: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, \
                                       provider_workspace_id, credential_invalid_at, credential_invalid_reason, refresh_token) \
             VALUES ($1, $2, $3, '', '\\x00', $4, CASE WHEN $5::text IS NULL THEN NULL ELSE now() END, $5, '\\x00')",
        )
        .bind(account)
        .bind(owner)
        .bind(provider)
        .bind(ws)
        .bind(invalid_reason)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reads_every_source_with_the_right_ownership_pg() {
        let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
            eprintln!(
                "skipping reads_every_source_with_the_right_ownership_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let opts = PgConnectOptions::from_str(&base).unwrap();
        let db =
            crate::test_db::migrated_database(&opts, crate::test_db::Plane::Control, "cs_status")
                .await;
        let pool = sqlx::pool::PoolOptions::<sqlx::Postgres>::new()
            .max_connections(4)
            .connect_with(opts.clone().database(&db))
            .await
            .unwrap();

        let (me, account) = user_account(&pool, "cs-status-me@test.local").await;
        let other: Uuid = sqlx::query_scalar(
            "INSERT INTO users (email) VALUES ('cs-status-other@test.local') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // Slack revoked (account-wide); another member's Gmail token must not show.
        token(
            &pool,
            account,
            None,
            "slack",
            "T1",
            Some("slack_token_revoked"),
        )
        .await;
        token(&pool, account, Some(me), "google_meet", "", None).await;
        token(&pool, account, Some(me), "google_gmail", "", None).await;
        token(&pool, account, Some(other), "google_gmail", "", None).await;
        token(&pool, account, None, "github", "gh", None).await;

        // Meet poller: my row errored after its last success; the other member's row is theirs.
        for (owner, err) in [(me, "quota exceeded\nstack"), (other, "should never show")] {
            sqlx::query(
                "INSERT INTO google_meet_sync_state (account_id, owner_user_id, last_synced_at, last_error, last_error_at) \
                 VALUES ($1, $2, now() - interval '2 hours', $3, now() - interval '1 hour')",
            )
            .bind(account)
            .bind(owner)
            .bind(err)
            .execute(&pool)
            .await
            .unwrap();
        }
        // Repositories: one healthy, one failing.
        let graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name) VALUES ($1, 'g') RETURNING id",
        )
        .bind(account)
        .fetch_one(&pool)
        .await
        .unwrap();
        for (repo_id, code) in [(1_i64, None), (2_i64, Some("destination_unreachable"))] {
            sqlx::query(
                "INSERT INTO github_watched_repos (account_id, installation_id, repo_id, owner, name, active, graph_id, \
                                                   destination_kind, last_synced_at, sync_error_code, sync_error_at) \
                 VALUES ($1, 9, $2, 'o', 'n', true, $3, 'personal', now() - interval '3 hours', $4, \
                         NULL)",
            )
            .bind(account)
            .bind(repo_id)
            .bind(graph)
            .bind(code)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO mcp_tool_calls (account_id, user_id, tool, outcome) VALUES ($1, $2, 'ask_ocean', 'ok')",
        )
        .bind(account)
        .bind(me)
        .execute(&pool)
        .await
        .unwrap();
        // Gmail watches are per member: mine is healthy, the other member's has
        // an error naming their address. Both members' Gmail atoms land in the
        // same ocean; only mine may count for me.
        for (owner, err) in [(me, None), (other, Some("other@example.com: quota"))] {
            sqlx::query(
                "INSERT INTO google_watch_channels \
                   (account_id, surface, resource_id, expires_at, owner_user_id, last_renewed_at, last_error, last_error_at) \
                 VALUES ($1, 'gmail', $2, now() + interval '5 days', $3, now(), $4, \
                         CASE WHEN $4::text IS NULL THEN NULL ELSE now() + interval '1 minute' END)",
            )
            .bind(account)
            .bind(owner.to_string())
            .bind(owner)
            .bind(err)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp, ingested_by) \
             VALUES ($1, 'gmail', 'm1', 'k4', now(), $2), \
                    ($1, 'gmail', 'o1', 'k5', now(), $3), ($1, 'gmail', 'o2', 'k6', now(), $3)",
        )
        .bind(graph)
        .bind(me)
        .bind(other)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, 'github_pr', 'a', 'k1', now()), ($1, 'github_pr', 'b', 'k2', now()), \
                    ($1, 'github_pr', 'old', 'k3', now())",
        )
        .bind(graph)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE atoms SET created_at = now() - interval '45 days' WHERE source_id = 'old'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut ctl = crate::tenancy::scope::begin_account_scoped(&pool, account)
            .await
            .unwrap();
        let inputs = fetch_control_inputs(&mut ctl, account, me).await.unwrap();
        ctl.rollback().await.ok();
        let providers: Vec<_> = inputs.oauth.iter().map(|r| r.provider.as_str()).collect();
        assert_eq!(
            providers,
            ["github", "google_gmail", "google_meet", "slack"],
            "other member's token must not appear"
        );

        let now = Utc::now();
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account)
            .await
            .unwrap();
        let atoms = fetch_atom_stats(&mut tx, account, me, &[graph], now)
            .await
            .unwrap();
        tx.rollback().await.ok();
        assert_eq!(
            atoms.by_source["github_pr"].0, 2,
            "the 45-day-old atom is outside the window"
        );
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account)
            .await
            .unwrap();
        let none = fetch_atom_stats(&mut tx, account, me, &[], now)
            .await
            .unwrap();
        tx.rollback().await.ok();
        assert!(
            none.by_source.is_empty(),
            "no readable oceans means no counts"
        );

        let out = assemble(&inputs, &atoms, None, now);
        let by: HashMap<_, _> = out
            .connectors
            .iter()
            .map(|r| (r.provider.as_str(), r))
            .collect();
        assert_eq!(by["slack"].state, State::Failing);
        assert_eq!(by["slack"].action, Some(Action::Reconnect));
        assert_eq!(by["google_meet"].state, State::Failing);
        assert_eq!(by["google_meet"].message.as_deref(), Some("quota exceeded"));
        assert_eq!(by["github"].state, State::Failing);
        assert_eq!(
            by["github"].message.as_deref(),
            Some("A repository's destination ocean is no longer reachable")
        );
        assert_eq!(by["github"].items_30d, 2);
        assert_eq!(by["mcp"].state, State::Healthy);
        // Two members: the other member's watch error never reaches me, and
        // their Gmail atoms do not count toward mine.
        assert_eq!(by["google_gmail"].state, State::Healthy);
        assert_eq!(by["google_gmail"].message, None);
        assert_eq!(by["google_gmail"].items_30d, 1);
        assert_eq!(atoms.by_source["gmail"].0, 3, "account-wide view sees all");
        assert!(!format!("{:?}", inputs.obs).contains("other@example.com"));

        // ...and from the other member's side the same data reads as theirs.
        let mut ctl = crate::tenancy::scope::begin_account_scoped(&pool, account)
            .await
            .unwrap();
        let theirs = fetch_control_inputs(&mut ctl, account, other)
            .await
            .unwrap();
        ctl.rollback().await.ok();
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account)
            .await
            .unwrap();
        let their_atoms = fetch_atom_stats(&mut tx, account, other, &[graph], now)
            .await
            .unwrap();
        tx.rollback().await.ok();
        let their_out = assemble(&theirs, &their_atoms, None, now);
        let gm = their_out
            .connectors
            .iter()
            .find(|r| r.provider == "google_gmail")
            .unwrap();
        assert_eq!(gm.state, State::Failing);
        assert_eq!(gm.message.as_deref(), Some("other@example.com: quota"));
        assert_eq!(gm.items_30d, 2);
        assert_eq!(
            out.summary,
            Summary {
                total: 5,
                healthy: 2,
                stale: 0,
                failing: 3
            }
        );
        assert_eq!(
            out.connectors.last().unwrap().provider,
            "mcp",
            "healthy rows sort last"
        );
    }
}
