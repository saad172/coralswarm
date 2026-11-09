//! The ten adapters, in display order: Slack, Microsoft, Google, GitHub, then
//! one generic adapter per Composio toolkit. Keep in step with the registry in
//! `connectors::mod` (the OAuth providers) and `composio::recipe` (the toolkits);
//! the tests below fail when they drift.

use std::sync::LazyLock;

use super::adapters::{
    ComposioAdapter, GithubAdapter, GoogleAdapter, MicrosoftAdapter, SlackAdapter,
};
use super::ConnectorAdapter;

/// Toolkit order on the grid. Linear first (the design's lead Beta card), then
/// the rest as the design lists them.
const TOOLKITS: [&str; 6] = ["linear", "jira", "confluence", "hubspot", "zendesk", "dart"];

static ADAPTERS: LazyLock<Vec<Box<dyn ConnectorAdapter>>> = LazyLock::new(|| {
    let mut all: Vec<Box<dyn ConnectorAdapter>> = vec![
        Box::new(SlackAdapter),
        Box::new(MicrosoftAdapter),
        Box::new(GoogleAdapter),
        Box::new(GithubAdapter),
    ];
    for slug in TOOLKITS {
        let adapter = ComposioAdapter::for_toolkit(slug)
            .unwrap_or_else(|| panic!("connector catalog: toolkit {slug} has no recipe"));
        all.push(Box::new(adapter));
    }
    all
});

/// Every adapter, in display order.
pub fn all() -> &'static [Box<dyn ConnectorAdapter>] {
    ADAPTERS.as_slice()
}

/// The adapter for a `{provider}` path segment; `None` is a 404.
pub fn lookup(key: &str) -> Option<&'static dyn ConnectorAdapter> {
    all().iter().find(|a| a.key() == key).map(|a| a.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::composio::recipe;
    use std::collections::HashSet;

    #[test]
    fn ten_adapters_in_display_order() {
        let keys: Vec<&str> = all().iter().map(|a| a.key()).collect();
        assert_eq!(
            keys,
            [
                "slack",
                "microsoft",
                "google",
                "github",
                "linear",
                "jira",
                "confluence",
                "hubspot",
                "zendesk",
                "dart"
            ]
        );
    }

    #[test]
    fn exactly_the_six_toolkits_are_beta() {
        let beta: Vec<&str> = all().iter().filter(|a| a.beta()).map(|a| a.key()).collect();
        assert_eq!(
            beta,
            ["linear", "jira", "confluence", "hubspot", "zendesk", "dart"]
        );
    }

    #[test]
    fn every_recipe_has_an_adapter_and_no_more() {
        let recipes: HashSet<&str> = recipe::all().map(|r| r.toolkit.as_str()).collect();
        let adapters: HashSet<&str> = TOOLKITS.into_iter().collect();
        assert_eq!(recipes, adapters);
    }

    #[test]
    fn oauth_adapters_match_the_provider_registry() {
        for p in crate::connectors::ALL {
            let a = lookup(p.id()).unwrap_or_else(|| panic!("no adapter for provider {}", p.id()));
            // The catalog claims every atom source type its provider owns, except
            // the ones that belong to no source a member can switch on.
            let claimed: HashSet<&str> = a
                .sources()
                .iter()
                .flat_map(|s| s.atom_source_types.iter().copied())
                .collect();
            for st in p.source_types() {
                assert!(
                    claimed.contains(st),
                    "{}: source type {st} is in no source",
                    p.id()
                );
            }
        }
    }

    #[test]
    fn keys_and_source_keys_are_unique_and_valid() {
        let mut keys = HashSet::new();
        for a in all() {
            assert!(keys.insert(a.key()), "duplicate connector {}", a.key());
            let mut sources = HashSet::new();
            for s in a.sources() {
                assert!(
                    sources.insert(s.key),
                    "{}: duplicate source {}",
                    a.key(),
                    s.key
                );
                // The same shape `connector_sources` enforces with a CHECK.
                assert!(
                    s.key
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                        && !s.key.is_empty()
                        && s.key.len() <= 64,
                    "{}: bad source key {}",
                    a.key(),
                    s.key
                );
            }
        }
    }

    #[test]
    fn source_types_are_not_shared_between_sources() {
        let mut seen: HashSet<&str> = HashSet::new();
        for a in all() {
            for s in a.sources() {
                for t in s.atom_source_types {
                    assert!(seen.insert(t), "{t} is claimed twice");
                }
            }
        }
    }

    #[test]
    fn scope_shapes_follow_the_decisions() {
        use crate::connectors::catalog::ScopeShape::*;
        let shape = |c: &str, s: &str| {
            lookup(c)
                .unwrap()
                .sources()
                .iter()
                .find(|x| x.key == s)
                .unwrap()
                .scope
        };
        // Decision 5: meetings pick only, calendars and toolkits everything only.
        assert!(matches!(
            shape("microsoft", "teams_meetings"),
            PickOnly { .. }
        ));
        assert!(matches!(shape("google", "meet"), PickOnly { .. }));
        assert!(matches!(shape("microsoft", "calendar"), EverythingOnly));
        assert!(matches!(shape("google", "calendar"), EverythingOnly));
        for toolkit in TOOLKITS {
            assert!(
                matches!(shape(toolkit, "workspace"), EverythingOnly),
                "{toolkit}"
            );
        }
        assert!(matches!(shape("slack", "channels"), Items { .. }));
        assert!(matches!(shape("github", "repos"), Items { .. }));
    }

    #[test]
    fn toolkits_cannot_backfill_and_say_so() {
        use crate::connectors::catalog::HistorySupport::*;
        for toolkit in TOOLKITS {
            let s = &lookup(toolkit).unwrap().sources()[0];
            assert!(
                matches!(
                    s.history,
                    Unsupported {
                        reason: "starts_at_connect"
                    }
                ),
                "{toolkit}"
            );
        }
    }

    #[test]
    fn no_display_name_says_composio() {
        for a in all() {
            let text = format!("{} {}", a.key(), a.display_name()).to_lowercase();
            assert!(!text.contains("composio"), "{text}");
            for s in a.sources() {
                assert!(!format!("{} {}", s.key, s.label)
                    .to_lowercase()
                    .contains("composio"));
            }
        }
    }
}
