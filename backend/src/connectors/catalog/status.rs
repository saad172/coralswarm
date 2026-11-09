//! The one derivation of connector and source status.
//!
//! Pure: no I/O, no clock. Adapters gather [`SourceInputs`] and [`GrantHealth`]
//! from provider tables; this module alone decides what the member is told.
//! `docs/design/connectors-redesign/PLAN.md`, "Backend data": connector
//! precedence is `reconnect_needed > needs_attention > importing > setting_up >
//! active`.

use serde::Serialize;

use super::GrantHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorStatus {
    NotConnected,
    SettingUp,
    Active,
    Importing,
    NeedsAttention,
    ReconnectNeeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    Off,
    Active,
    Importing,
    Paused,
    NeedsAttention,
    Stopped,
}

/// Where the wizard resumes a half-finished setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResumeStep {
    Sources,
    Scope,
    History,
}

/// What the status of one source is derived from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceInputs {
    pub enabled: bool,
    /// The member paused this source.
    pub paused: bool,
    /// Capture ended provider-side (an uninstalled or suspended app).
    pub stopped: bool,
    pub importing: bool,
    pub has_problem: bool,
    /// The sign-in is dead: nothing captures until the member reconnects.
    pub grant_dead: bool,
}

/// One source's status. Off beats everything: a source the member did not turn
/// on has nothing to report. A dead sign-in and a provider-side stop both read
/// "stopped". The member's own pause outranks a problem, since pausing is a
/// deliberate act and the problem is moot until they resume.
pub fn source_status(i: SourceInputs) -> SourceStatus {
    if !i.enabled {
        SourceStatus::Off
    } else if i.grant_dead || i.stopped {
        SourceStatus::Stopped
    } else if i.paused {
        SourceStatus::Paused
    } else if i.has_problem {
        SourceStatus::NeedsAttention
    } else if i.importing {
        SourceStatus::Importing
    } else {
        SourceStatus::Active
    }
}

/// What the connector's status is derived from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectorInputs<'a> {
    pub connected: bool,
    pub grant: GrantKind,
    pub sources: &'a [SourceStatus],
    pub setup_complete: bool,
}

/// [`GrantHealth`] without its payload, for the pure derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantKind {
    Healthy,
    ReconnectNeeded,
    NeedsAttention,
}

impl From<&GrantHealth> for GrantKind {
    fn from(h: &GrantHealth) -> Self {
        match h {
            GrantHealth::Healthy => Self::Healthy,
            GrantHealth::ReconnectNeeded(_) => Self::ReconnectNeeded,
            GrantHealth::NeedsAttention(_) => Self::NeedsAttention,
        }
    }
}

/// The connector's status. Precedence, highest first: not connected, reconnect
/// needed, needs attention (the sign-in, or any source), importing (any source),
/// setting up, active.
pub fn connector_status(i: ConnectorInputs<'_>) -> ConnectorStatus {
    if !i.connected {
        return ConnectorStatus::NotConnected;
    }
    if i.grant == GrantKind::ReconnectNeeded {
        return ConnectorStatus::ReconnectNeeded;
    }
    if i.grant == GrantKind::NeedsAttention || i.sources.contains(&SourceStatus::NeedsAttention) {
        return ConnectorStatus::NeedsAttention;
    }
    if i.sources.contains(&SourceStatus::Importing) {
        return ConnectorStatus::Importing;
    }
    if !i.setup_complete {
        return ConnectorStatus::SettingUp;
    }
    ConnectorStatus::Active
}

/// What one enabled source still needs from the member during setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupInputs {
    pub enabled: bool,
    pub setup_completed: bool,
    /// The source lists items or modes, so a scope has to be chosen.
    pub needs_scope_choice: bool,
    pub scope_chosen: bool,
    /// The source can import history, so the step exists for it.
    pub can_backfill: bool,
    pub history_chosen: bool,
}

/// Whether setup is done: at least one source is on and every source that is on
/// has finished. A connection that predates the catalog has no per-source rows;
/// its adapter reports each source it can see as finished, so it is set up.
pub fn setup_complete(sources: &[SetupInputs]) -> bool {
    let mut any_on = false;
    for s in sources.iter().filter(|s| s.enabled) {
        any_on = true;
        if !s.setup_completed {
            return false;
        }
    }
    any_on
}

/// The first wizard step that still has work, or `None` once setup is done.
/// Order follows the lifecycle: sources, then scope, then history.
pub fn resume_step(sources: &[SetupInputs], setup_done: bool) -> Option<ResumeStep> {
    if setup_done {
        return None;
    }
    let on: Vec<&SetupInputs> = sources.iter().filter(|s| s.enabled).collect();
    if on.is_empty() {
        return Some(ResumeStep::Sources);
    }
    if on.iter().any(|s| s.needs_scope_choice && !s.scope_chosen) {
        return Some(ResumeStep::Scope);
    }
    if on.iter().any(|s| s.can_backfill && !s.history_chosen) {
        return Some(ResumeStep::History);
    }
    // Every choice is made but the member has not confirmed: the last step.
    Some(ResumeStep::History)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ConnectorStatus as C;
    use SourceStatus as S;

    fn si(f: impl FnOnce(&mut SourceInputs)) -> SourceInputs {
        let mut i = SourceInputs {
            enabled: true,
            ..Default::default()
        };
        f(&mut i);
        i
    }

    #[test]
    fn source_status_table() {
        let cases: Vec<(&str, SourceInputs, SourceStatus)> = vec![
            ("not enabled", SourceInputs::default(), S::Off),
            (
                "off beats a problem",
                SourceInputs {
                    has_problem: true,
                    ..Default::default()
                },
                S::Off,
            ),
            (
                "off beats a dead grant",
                SourceInputs {
                    grant_dead: true,
                    ..Default::default()
                },
                S::Off,
            ),
            ("plain", si(|_| {}), S::Active),
            ("importing", si(|i| i.importing = true), S::Importing),
            ("problem", si(|i| i.has_problem = true), S::NeedsAttention),
            ("paused", si(|i| i.paused = true), S::Paused),
            (
                "stopped provider-side",
                si(|i| i.stopped = true),
                S::Stopped,
            ),
            ("dead grant", si(|i| i.grant_dead = true), S::Stopped),
            (
                "problem beats importing",
                si(|i| {
                    i.has_problem = true;
                    i.importing = true;
                }),
                S::NeedsAttention,
            ),
            (
                "paused beats problem",
                si(|i| {
                    i.paused = true;
                    i.has_problem = true;
                }),
                S::Paused,
            ),
            (
                "paused beats importing",
                si(|i| {
                    i.paused = true;
                    i.importing = true;
                }),
                S::Paused,
            ),
            (
                "stopped beats paused",
                si(|i| {
                    i.stopped = true;
                    i.paused = true;
                }),
                S::Stopped,
            ),
            (
                "dead grant beats everything on",
                si(|i| {
                    i.grant_dead = true;
                    i.paused = true;
                    i.has_problem = true;
                    i.importing = true;
                }),
                S::Stopped,
            ),
        ];
        for (name, input, want) in cases {
            assert_eq!(source_status(input), want, "{name}");
        }
    }

    fn ci<'a>(
        connected: bool,
        grant: GrantKind,
        sources: &'a [SourceStatus],
        setup_complete: bool,
    ) -> ConnectorInputs<'a> {
        ConnectorInputs {
            connected,
            grant,
            sources,
            setup_complete,
        }
    }

    #[test]
    fn connector_status_table() {
        use GrantKind::*;
        let cases: Vec<(&str, ConnectorInputs<'_>, ConnectorStatus)> = vec![
            (
                "not connected",
                ci(false, Healthy, &[], true),
                C::NotConnected,
            ),
            (
                "not connected beats a stale grant kind",
                ci(false, ReconnectNeeded, &[S::Importing], false),
                C::NotConnected,
            ),
            (
                "dead sign-in",
                ci(true, ReconnectNeeded, &[S::Stopped], true),
                C::ReconnectNeeded,
            ),
            (
                "reconnect beats attention",
                ci(true, ReconnectNeeded, &[S::NeedsAttention], true),
                C::ReconnectNeeded,
            ),
            (
                "reconnect beats importing and setup",
                ci(true, ReconnectNeeded, &[S::Importing], false),
                C::ReconnectNeeded,
            ),
            (
                "sign-in needs attention",
                ci(true, NeedsAttention, &[S::Active], true),
                C::NeedsAttention,
            ),
            (
                "a source needs attention",
                ci(true, Healthy, &[S::Active, S::NeedsAttention], true),
                C::NeedsAttention,
            ),
            (
                "attention beats importing",
                ci(true, Healthy, &[S::Importing, S::NeedsAttention], true),
                C::NeedsAttention,
            ),
            (
                "attention beats setting up",
                ci(true, Healthy, &[S::NeedsAttention], false),
                C::NeedsAttention,
            ),
            (
                "importing",
                ci(true, Healthy, &[S::Active, S::Importing], true),
                C::Importing,
            ),
            (
                "importing beats setting up",
                ci(true, Healthy, &[S::Importing], false),
                C::Importing,
            ),
            (
                "setting up",
                ci(true, Healthy, &[S::Active], false),
                C::SettingUp,
            ),
            (
                "setting up with nothing on",
                ci(true, Healthy, &[S::Off], false),
                C::SettingUp,
            ),
            (
                "active",
                ci(true, Healthy, &[S::Active, S::Off], true),
                C::Active,
            ),
            (
                "a paused source does not demote the connector",
                ci(true, Healthy, &[S::Paused, S::Active], true),
                C::Active,
            ),
            (
                "a stopped source alone stays active",
                ci(true, Healthy, &[S::Stopped, S::Active], true),
                C::Active,
            ),
        ];
        for (name, input, want) in cases {
            assert_eq!(connector_status(input), want, "{name}");
        }
    }

    fn su(f: impl FnOnce(&mut SetupInputs)) -> SetupInputs {
        let mut s = SetupInputs {
            enabled: true,
            setup_completed: false,
            needs_scope_choice: true,
            scope_chosen: false,
            can_backfill: true,
            history_chosen: false,
        };
        f(&mut s);
        s
    }

    #[test]
    fn setup_needs_a_source_on_and_every_source_on_finished() {
        assert!(!setup_complete(&[]), "nothing is on");
        assert!(!setup_complete(&[su(|s| s.enabled = false)]));
        assert!(!setup_complete(&[su(|_| {})]));
        assert!(!setup_complete(&[
            su(|s| s.setup_completed = true),
            su(|_| {})
        ]));
        assert!(setup_complete(&[su(|s| s.setup_completed = true)]));
        assert!(
            setup_complete(&[su(|s| s.setup_completed = true), su(|s| s.enabled = false)]),
            "an off source never blocks setup"
        );
    }

    #[test]
    fn resume_step_follows_the_lifecycle() {
        assert_eq!(resume_step(&[], true), None);
        assert_eq!(
            resume_step(&[su(|s| s.enabled = false)], false),
            Some(ResumeStep::Sources)
        );
        assert_eq!(resume_step(&[su(|_| {})], false), Some(ResumeStep::Scope));
        assert_eq!(
            resume_step(&[su(|s| s.scope_chosen = true)], false),
            Some(ResumeStep::History)
        );
        assert_eq!(
            resume_step(
                &[su(|s| {
                    s.scope_chosen = true;
                    s.history_chosen = true;
                })],
                false
            ),
            Some(ResumeStep::History),
            "all chosen but unconfirmed lands on the last step"
        );
    }

    #[test]
    fn resume_skips_steps_a_source_does_not_have() {
        // Everything-only, no backfill: nothing to choose, so the last step.
        let only = su(|s| {
            s.needs_scope_choice = false;
            s.can_backfill = false;
        });
        assert_eq!(resume_step(&[only], false), Some(ResumeStep::History));
        // Items but no backfill: scope first.
        let items = su(|s| s.can_backfill = false);
        assert_eq!(resume_step(&[items], false), Some(ResumeStep::Scope));
    }

    #[test]
    fn wire_values_are_snake_case() {
        assert_eq!(
            serde_json::to_value(C::NotConnected).unwrap(),
            "not_connected"
        );
        assert_eq!(
            serde_json::to_value(C::ReconnectNeeded).unwrap(),
            "reconnect_needed"
        );
        assert_eq!(
            serde_json::to_value(S::NeedsAttention).unwrap(),
            "needs_attention"
        );
        assert_eq!(
            serde_json::to_value(ResumeStep::Sources).unwrap(),
            "sources"
        );
    }
}
