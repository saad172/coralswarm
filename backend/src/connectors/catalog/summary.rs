//! The wire shapes of `GET /v1/connectors[/{provider}]` and the one function that
//! assembles them from an adapter. `docs/design/connectors-redesign/PLAN.md`,
//! "Routes" and the TypeScript block under it.
//!
//! Seams left for later layers, all explicit here and nowhere else:
//! Activity, freshness and counts use the Library-admitted catalog activity reader.
//! Topics join only admitted captures to `node_atoms`.
//! * `scope.count` is filled when B3 stores the picked items.
//! * `backfill` for Slack and the other providers is filled by B4; GitHub reports
//!   its existing per-repo progress already.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use super::history;
use super::status::{
    connector_status, resume_step, setup_complete, source_status, ConnectorInputs, ConnectorStatus,
    GrantKind, ResumeStep, SetupInputs, SourceInputs, SourceStatus,
};
use super::{
    AdapterCx, AdapterError, ConnectorAdapter, GrantHealth, GrantView, HistorySupport, Problem,
    ScopeMode, SourceRuntime, SourceSpec,
};

#[derive(Debug, Serialize, ToSchema)]
pub struct ScopeView {
    #[schema(value_type = String)]
    pub mode: ScopeMode,
    /// Items picked. `null` until B3 stores them.
    pub count: Option<u32>,
    /// The plural noun the picker counts. `null` for a source with no items.
    pub noun: Option<&'static str>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
pub enum HistoryView {
    Supported {
        supported: bool,
        max_days: u32,
    },
    Unsupported {
        supported: bool,
        reason: &'static str,
    },
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BackfillView {
    /// `null` is indeterminate.
    pub percent: Option<u8>,
    /// `0` is an automatic, unbounded window.
    pub window_days: u32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SourceSummary {
    pub key: &'static str,
    pub label: &'static str,
    pub status: SourceStatus,
    pub enabled: bool,
    pub paused: bool,
    pub scope_shape: &'static str,
    pub scope: Option<ScopeView>,
    pub history: HistoryView,
    /// Persisted choice: null means no choice; zero means None.
    pub history_days: Option<i32>,
    /// Durable request claim. Clients must reconcile this before retrying a lost response.
    #[schema(value_type = Option<String>, format = DateTime)]
    pub history_requested_at: Option<DateTime<Utc>>,
    #[schema(value_type = Option<String>, format = DateTime)]
    pub setup_completed_at: Option<DateTime<Utc>>,
    #[schema(value_type = Option<String>, format = DateTime)]
    pub last_captured_at: Option<DateTime<Utc>>,
    /// Atoms captured per day over the last seven days, oldest first.
    pub week: [u32; 7],
    pub backfill: Option<BackfillView>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ImportingView {
    pub source: &'static str,
    pub window_days: u32,
    pub percent: Option<u8>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectorSummary {
    pub key: &'static str,
    pub name: &'static str,
    pub beta: bool,
    pub status: ConnectorStatus,
    pub sources_on: u32,
    pub sources_total: u32,
    #[schema(value_type = Option<String>, format = DateTime)]
    pub last_captured_at: Option<DateTime<Utc>>,
    pub importing: Option<ImportingView>,
    pub problem: Option<Problem>,
    pub resume_step: Option<ResumeStep>,
    pub sources: Vec<SourceSummary>,
}

/// A topic captured through this connector (B5).
#[derive(Debug, Serialize, ToSchema)]
pub struct TopicSummary {
    pub id: Uuid,
    pub label: String,
    pub total: u32,
    pub last_7_days: u32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectorDetail {
    #[serde(flatten)]
    pub summary: ConnectorSummary,
    pub account_label: Option<String>,
    #[schema(value_type = Option<String>, format = DateTime)]
    pub connected_at: Option<DateTime<Utc>>,
    pub topics: Vec<TopicSummary>,
}

fn history_view(h: HistorySupport) -> HistoryView {
    match h {
        HistorySupport::Backfill { max_days } => HistoryView::Supported {
            supported: true,
            max_days,
        },
        HistorySupport::Unsupported { reason } => HistoryView::Unsupported {
            supported: false,
            reason,
        },
    }
}

fn scope_view(rt: &SourceRuntime, spec: &SourceSpec) -> Option<ScopeView> {
    rt.scope.map(|s| ScopeView {
        mode: s.mode,
        count: s.count,
        noun: spec.scope.noun(),
    })
}

/// Assemble one connector's summary and, for the detail route, its grant.
pub async fn build(
    adapter: &dyn ConnectorAdapter,
    cx: &AdapterCx<'_>,
) -> Result<(ConnectorSummary, Option<GrantView>), AdapterError> {
    let activity = super::activity::ActivityByType::load_action(cx).await?;
    let mut runtimes = std::collections::HashMap::new();
    if adapter.grant_facts(cx.facts)?.is_some() {
        for spec in adapter.sources() {
            let mut rt = adapter.source_runtime(cx, spec).await?;
            history::progress(cx, adapter.key(), spec, &mut rt).await?;
            runtimes.insert((adapter.key().into(), spec.key.into()), rt);
        }
    }
    build_stored(adapter, cx.facts, &runtimes, &activity)
}

/// Assemble the same wire response from worker-prepared facts. This function
/// performs no SQL and has no access to provider clients or mutable pools.
pub fn build_stored(
    adapter: &dyn ConnectorAdapter,
    facts: &super::Facts,
    runtimes: &std::collections::HashMap<(String, String), SourceRuntime>,
    activity: &super::activity::ActivityByType,
) -> Result<(ConnectorSummary, Option<GrantView>), AdapterError> {
    let grant = adapter.grant_facts(facts)?;
    let connected = grant.is_some();
    let kind = grant
        .as_ref()
        .map_or(GrantKind::Healthy, |g| GrantKind::from(&g.health));

    let specs = adapter.sources();
    let mut sources = Vec::with_capacity(specs.len());
    let mut statuses = Vec::with_capacity(specs.len());
    let mut setup = Vec::with_capacity(specs.len());
    let mut importing = None;
    let mut source_problem: Option<Problem> = None;
    let mut sources_on = 0u32;

    for spec in specs {
        // Nothing is on, scoped or running without a sign-in.
        let rt = if connected {
            runtimes
                .get(&(adapter.key().into(), spec.key.into()))
                .cloned()
                .unwrap_or_default()
        } else {
            SourceRuntime::default()
        };
        let status = source_status(SourceInputs {
            enabled: rt.enabled,
            paused: rt.paused,
            stopped: rt.stopped,
            importing: rt.importing.is_some(),
            has_problem: rt.problem.is_some(),
            grant_dead: kind == GrantKind::ReconnectNeeded,
        });
        if rt.enabled {
            sources_on += 1;
        }
        if status == SourceStatus::Importing && importing.is_none() {
            if let Some(i) = rt.importing {
                importing = Some(ImportingView {
                    source: spec.key,
                    window_days: i.window_days,
                    percent: i.percent,
                });
            }
        }
        if status == SourceStatus::NeedsAttention && source_problem.is_none() {
            source_problem = rt.problem.clone();
        }
        setup.push(SetupInputs {
            enabled: rt.enabled,
            setup_completed: rt.setup_completed,
            // Unsupported sources finish only after scope confirmation, including
            // Everything-only sources and rows saved by older wizard versions.
            needs_scope_choice: spec.scope.needs_scope_choice()
                || matches!(spec.history, HistorySupport::Unsupported { .. }),
            scope_chosen: rt.scope.is_some()
                && (matches!(spec.history, HistorySupport::Backfill { .. }) || rt.setup_completed),
            can_backfill: matches!(spec.history, HistorySupport::Backfill { .. }),
            history_chosen: rt.history_days.is_some(),
        });
        let (last_captured_at, week) = activity.stats(spec.atom_source_types);
        sources.push(SourceSummary {
            key: spec.key,
            label: spec.label,
            status,
            enabled: rt.enabled,
            paused: rt.paused,
            scope_shape: spec.scope.wire(),
            scope: scope_view(&rt, spec),
            history: history_view(spec.history),
            history_days: facts
                .source(adapter.key(), spec.key)
                .and_then(|r| r.history_days),
            history_requested_at: facts
                .source(adapter.key(), spec.key)
                .and_then(|r| r.history_requested_at),
            setup_completed_at: facts
                .source(adapter.key(), spec.key)
                .and_then(|r| r.setup_completed_at),
            last_captured_at,
            week,
            backfill: rt.importing.map(|i| BackfillView {
                percent: i.percent,
                window_days: i.window_days,
            }),
        });
        statuses.push(status);
    }

    let setup_done = setup_complete(&setup);
    let status = connector_status(ConnectorInputs {
        connected,
        grant: kind,
        sources: &statuses,
        setup_complete: setup_done,
    });
    let grant_problem = grant.as_ref().and_then(|g| match &g.health {
        GrantHealth::Healthy => None,
        GrantHealth::ReconnectNeeded(p) | GrantHealth::NeedsAttention(p) => Some(p.clone()),
    });

    let summary = ConnectorSummary {
        key: adapter.key(),
        name: adapter.display_name(),
        beta: adapter.beta(),
        status,
        sources_on,
        sources_total: specs.len() as u32,
        last_captured_at: sources.iter().filter_map(|s| s.last_captured_at).max(),
        importing,
        problem: grant_problem.or(source_problem),
        resume_step: if connected {
            resume_step(&setup, setup_done)
        } else {
            None
        },
        sources,
    };
    Ok((summary, grant))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::catalog::facts::Facts;
    use crate::connectors::catalog::{registry, ScopeShape};

    use crate::middleware::auth::UserContext;
    use crate::tenancy::extractor::Tenant;
    use std::sync::{Arc, LazyLock};

    // The read side never touches these; the write side's tests are in
    // `connect.rs` and the pg suite.
    static STATE: LazyLock<Arc<crate::AppState>> = LazyLock::new(crate::test_app_state);
    static TENANT: LazyLock<Tenant> = LazyLock::new(|| Tenant {
        store: None,
        pool: None,
        decision: None,
        class: crate::route_class::RouteClass::Other,
    });
    static USER: LazyLock<UserContext> = LazyLock::new(|| UserContext {
        user_id: Uuid::nil(),
        account_id: Uuid::nil(),
        role: "member".into(),
        device_id: String::new(),
    });

    fn cx(facts: &Facts) -> AdapterCx<'_> {
        AdapterCx::new(facts, &STATE, &TENANT, &USER)
    }

    #[tokio::test]
    async fn history_choice_survives_remount_without_replaying_none() {
        use crate::connectors::catalog::facts::SourceRow;
        for days in [0, 7, 30, 90] {
            let now = Utc::now();
            let facts = Facts {
                sources: vec![SourceRow {
                    provider: "microsoft".into(),
                    source: "teams_chats".into(),
                    enabled: true,
                    paused_at: None,
                    scope_mode: Some("everything".into()),
                    history_days: Some(days),
                    history_requested_at: Some(now),
                    setup_completed_at: Some(now),
                }],
                ..Default::default()
            };
            for _remount in 0..2 {
                let (summary, _) = build(registry::lookup("microsoft").unwrap(), &cx(&facts))
                    .await
                    .unwrap();
                let source = summary
                    .sources
                    .iter()
                    .find(|s| s.key == "teams_chats")
                    .unwrap();
                assert_eq!(source.history_days, Some(days));
                assert_eq!(source.history_requested_at, Some(now));
                assert_eq!(source.setup_completed_at, Some(now));
            }
        }
    }

    #[tokio::test]
    async fn a_fresh_member_has_ten_not_connected_connectors() {
        let facts = Facts::default();
        let cx = cx(&facts);
        let mut beta = 0;
        for a in registry::all() {
            let (s, grant) = build(a.as_ref(), &cx).await.unwrap();
            assert!(grant.is_none(), "{}", s.key);
            assert_eq!(s.status, ConnectorStatus::NotConnected, "{}", s.key);
            assert_eq!(s.sources_on, 0);
            assert_eq!(s.sources_total as usize, s.sources.len());
            assert!(s.problem.is_none() && s.importing.is_none() && s.resume_step.is_none());
            assert!(s
                .sources
                .iter()
                .all(|x| x.status == SourceStatus::Off && !x.enabled));
            if s.beta {
                beta += 1;
            }
        }
        assert_eq!(beta, 6);
    }

    #[tokio::test]
    async fn payloads_never_say_composio() {
        let facts = Facts::default();
        let cx = cx(&facts);
        for a in registry::all() {
            let (s, _) = build(a.as_ref(), &cx).await.unwrap();
            let json = serde_json::to_string(&s).unwrap().to_lowercase();
            assert!(!json.contains("composio"), "{json}");
        }
    }

    #[tokio::test]
    async fn a_dead_microsoft_sign_in_stops_every_source_that_is_on() {
        use crate::connectors::catalog::facts::TokenRow;
        let now = Utc::now();
        let facts = Facts {
            tokens: vec![TokenRow {
                provider: "microsoft".into(),
                provider_workspace_id: None,
                scope: "x".into(),
                created_at: now,
                credential_invalid_at: Some(now),
                credential_invalid_reason: Some("ms_grant_expired".into()),
                mine: true,
            }],
            ..Default::default()
        };
        let cx = cx(&facts);
        let (s, g) = build(registry::lookup("microsoft").unwrap(), &cx)
            .await
            .unwrap();
        assert!(g.is_some());
        assert_eq!(s.status, ConnectorStatus::ReconnectNeeded);
        assert_eq!(s.problem.as_ref().unwrap().code, "credential_invalid");
        assert!(s.sources.iter().all(|x| x.status == SourceStatus::Stopped));
    }

    #[tokio::test]
    async fn revoked_tenant_consent_needs_attention_not_reconnect() {
        use crate::connectors::catalog::facts::TokenRow;
        let now = Utc::now();
        let facts = Facts {
            tokens: vec![TokenRow {
                provider: "microsoft".into(),
                provider_workspace_id: None,
                scope: "x".into(),
                created_at: now,
                credential_invalid_at: Some(now),
                credential_invalid_reason: Some("ms_consent_revoked".into()),
                mine: true,
            }],
            ..Default::default()
        };
        let (s, _) = build(registry::lookup("microsoft").unwrap(), &cx(&facts))
            .await
            .unwrap();
        assert_eq!(s.status, ConnectorStatus::NeedsAttention);
        assert_eq!(s.problem.unwrap().code, "tenant_consent_required");
    }

    #[tokio::test]
    async fn a_connected_toolkit_is_active_with_one_everything_source() {
        use crate::connectors::catalog::facts::ComposioRow;
        let now = Utc::now();
        let facts = Facts {
            composio: vec![ComposioRow {
                service: "jira".into(),
                status: "active".into(),
                provider_namespace: Some("jira:site:x".into()),
                created_at: now,
                updated_at: now,
            }],
            ..Default::default()
        };
        let (s, _) = build(registry::lookup("jira").unwrap(), &cx(&facts))
            .await
            .unwrap();
        assert_eq!(s.status, ConnectorStatus::Active);
        assert_eq!((s.sources_on, s.sources_total), (1, 1));
        let src = &s.sources[0];
        assert_eq!(src.scope.as_ref().unwrap().mode, ScopeMode::Everything);
        assert!(matches!(
            src.history,
            HistoryView::Unsupported {
                supported: false,
                ..
            }
        ));
        assert!(!serde_json::to_string(&s)
            .unwrap()
            .to_lowercase()
            .contains("composio"));
    }

    #[tokio::test]
    async fn github_with_an_install_but_no_repos_is_setting_up_at_sources() {
        use crate::connectors::catalog::facts::InstallRow;
        let now = Utc::now();
        let facts = Facts {
            installs: vec![InstallRow {
                installation_id: 1,
                account_login: "acme".into(),
                suspended_at: None,
                created_at: now,
                mine: false,
            }],
            ..Default::default()
        };
        let (s, g) = build(registry::lookup("github").unwrap(), &cx(&facts))
            .await
            .unwrap();
        assert_eq!(g.unwrap().account_label.as_deref(), Some("acme"));
        assert_eq!(s.status, ConnectorStatus::SettingUp);
        assert_eq!(s.resume_step, Some(ResumeStep::Sources));
    }

    #[tokio::test]
    async fn a_github_install_the_member_made_needs_their_token_to_count() {
        use crate::connectors::catalog::facts::{InstallRow, TokenRow};
        let now = Utc::now();
        let install = InstallRow {
            installation_id: 1,
            account_login: "acme".into(),
            suspended_at: None,
            created_at: now,
            mine: true,
        };
        // Disconnected: the token is gone, the App is still installed at GitHub.
        let gone = Facts {
            installs: vec![install.clone()],
            ..Default::default()
        };
        let (s, g) = build(registry::lookup("github").unwrap(), &cx(&gone))
            .await
            .unwrap();
        assert!(g.is_none());
        assert_eq!(s.status, ConnectorStatus::NotConnected);

        // Connected: the member holds their own token.
        let held = Facts {
            installs: vec![install],
            tokens: vec![TokenRow {
                provider: "github".into(),
                provider_workspace_id: Some("octocat".into()),
                scope: String::new(),
                created_at: now,
                credential_invalid_at: None,
                credential_invalid_reason: None,
                mine: true,
            }],
            ..Default::default()
        };
        let (s, g) = build(registry::lookup("github").unwrap(), &cx(&held))
            .await
            .unwrap();
        assert!(g.is_some());
        assert_eq!(s.status, ConnectorStatus::SettingUp);
    }

    #[tokio::test]
    async fn github_backfill_in_flight_is_importing() {
        use crate::connectors::catalog::facts::InstallRow;
        use crate::connectors::catalog::github_facts::GithubRepoFacts;
        let now = Utc::now();
        let facts = Facts {
            installs: vec![InstallRow {
                installation_id: 1,
                account_login: "acme".into(),
                suspended_at: None,
                created_at: now,
                mine: false,
            }],
            github: GithubRepoFacts {
                active: 2,
                importing_percent: Some(30),
                error: None,
            },
            ..Default::default()
        };
        let (s, _) = build(registry::lookup("github").unwrap(), &cx(&facts))
            .await
            .unwrap();
        assert_eq!(s.status, ConnectorStatus::Importing);
        let i = s.importing.unwrap();
        assert_eq!((i.source, i.percent), ("repos", Some(30)));
    }

    #[test]
    fn scope_shape_wire_values() {
        assert_eq!(ScopeShape::Items { noun: "x" }.wire(), "items");
        assert_eq!(ScopeShape::PickOnly { noun: "x" }.wire(), "pick_only");
        assert_eq!(ScopeShape::EverythingOnly.wire(), "everything_only");
    }
}
