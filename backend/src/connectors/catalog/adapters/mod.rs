//! One module per provider. Each adapter reads the provider's own tables through
//! [`Facts`](super::Facts) and answers the catalog's questions; the shared
//! helpers below keep the row-to-runtime mapping in one place.

mod composio;
mod github;
pub(crate) mod google;
pub(crate) mod microsoft;
mod slack;

pub use composio::ComposioAdapter;
pub use github::GithubAdapter;
pub use google::GoogleAdapter;
pub use microsoft::MicrosoftAdapter;
pub use slack::SlackAdapter;

use super::facts::{SourceRow, TokenRow};
use super::{
    scope, GrantHealth, GrantView, ParamValue, Problem, ScopeMode, ScopeShape, ScopeState,
    SourceRuntime, SourceSpec,
};

/// A dead-credential problem. `reason` is the stored
/// `CredentialDeathReason::code()`: a stable machine value, not prose.
pub(super) fn credential_problem(t: &TokenRow) -> Option<Problem> {
    let at = t.credential_invalid_at?;
    let mut p = Problem::new("credential_invalid", at);
    if let Some(reason) = &t.credential_invalid_reason {
        p = p.with_param("reason", ParamValue::Str(reason.clone()));
    }
    Some(p)
}

/// A grant for one `oauth_tokens` row: reconnect-needed when the credential is
/// marked dead, healthy otherwise.
pub(super) fn grant_from_token(t: &TokenRow, label: Option<String>) -> GrantView {
    GrantView {
        account_label: label,
        connected_at: t.created_at,
        health: match credential_problem(t) {
            Some(p) => GrantHealth::ReconnectNeeded(p),
            None => GrantHealth::Healthy,
        },
    }
}

/// Fold a source's stored row into its runtime. `legacy_enabled` answers "is the
/// source on?" for a member with no `connector_sources` row: every connection
/// made before the catalog, whose provider tables are the only record. A row, once
/// present, is authoritative.
///
/// The scope is the stored mode when there is one. A connection from before the
/// catalog (no row, or a row `connect::keep_legacy_connection_set_up` wrote, with
/// setup finished and no mode) reads the mode it always worked in
/// ([`scope::legacy_mode`]: Slack took everything, the rest an explicit list).
/// A source with nothing to pick has exactly one scope, so it is never unset.
/// Anything else has not been chosen yet. `count` is the adapter's to fill.
pub(crate) fn runtime_from_row(
    row: Option<&SourceRow>,
    spec: &SourceSpec,
    provider: &str,
    legacy_enabled: bool,
) -> SourceRuntime {
    let enabled = row.map_or(legacy_enabled, |r| r.enabled);
    let stored = row
        .and_then(|r| r.scope_mode.as_deref())
        .and_then(ScopeMode::parse);
    let predates_catalog = match row {
        None => legacy_enabled,
        Some(r) => r.setup_completed_at.is_some(),
    };
    let mode = match (stored, spec.scope) {
        (Some(mode), _) => Some(mode),
        (None, ScopeShape::EverythingOnly) if enabled => Some(ScopeMode::Everything),
        (None, shape) if predates_catalog => Some(scope::legacy_mode(provider, shape)),
        _ => None,
    };
    SourceRuntime {
        enabled,
        paused: row.is_some_and(|r| r.paused_at.is_some()),
        scope: mode.map(|mode| ScopeState { mode, count: None }),
        history_days: row.and_then(|r| r.history_days).map(|d| d as u32),
        setup_completed: row.map_or(enabled, |r| r.setup_completed_at.is_some()),
        ..Default::default()
    }
}

/// Set the picked count on a runtime whose scope is "Only what I pick". A mode
/// of "everything" has no count (`null` on the wire).
pub(crate) fn with_count(mut rt: SourceRuntime, count: u32) -> SourceRuntime {
    if let Some(s) = rt.scope.as_mut() {
        if s.mode == ScopeMode::Pick {
            s.count = Some(count);
        }
    }
    rt
}
