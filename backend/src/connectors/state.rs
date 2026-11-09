//! Operational STATE — CoralSwarm's awareness of its *own* configuration and
//! coverage for one (account, ocean): which connectors are connected, which
//! channels/threads it ingests, and how fresh / how much data each source has.
//!
//! This block is injected into synthesis (via [`crate::self_context`]) so the
//! ask path can answer "what's connected?", "which channels are you ingesting?",
//! "how recent is your Slack data?" — and, on a knowledge miss, self-qualify the
//! abstention ("I only have Slack since Jun 14") instead of a blank refusal.
//!
//! # Connector-agnostic by construction
//! The builder never names a specific connector. A new connector appears here for
//! free once it declares [`ConnectorProvider::source_types`]; per-connector code
//! is opt-in (override [`ConnectorProvider::describe_containers`]) only when a
//! connector tracks containers outside `atoms` (e.g. watched-but-not-yet-ingested
//! resources). See `docs/plans/operational-state-context.md`.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Cap on channels/threads listed per connector in the rendered block, so a
/// workspace with hundreds of channels can't bloat the synthesis prompt. The
/// most-recently-ingested win; the rest collapse to "+N more".
const MAX_CONTAINERS_RENDERED: usize = 30;

/// One ingested container (a Slack channel, Teams channel/chat, Outlook thread).
#[derive(Debug, Clone)]
pub struct Container {
    /// "channel" | "chat" | "thread".
    pub kind: String,
    /// Display label — "#eng", a chat/thread name, or an id when unnamed.
    pub label: String,
    pub last_ingested: Option<DateTime<Utc>>,
}

/// Per-source coverage row (one `source_type` worth of atoms in this ocean).
#[derive(Debug, Clone)]
pub struct SourceCoverage {
    pub source_type: String,
    pub count: i64,
    pub earliest: Option<DateTime<Utc>>,
    pub latest: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHealth {
    Connected,
    NeedsReauth,
}

/// One connector's contribution to the operational-state block.
#[derive(Debug, Clone)]
pub struct ConnectorState {
    pub display_name: String,
    /// Present in `oauth_tokens` for this account OR has coverage atoms.
    pub connected: bool,
    pub connected_at: Option<DateTime<Utc>>,
    pub health: ConnectionHealth,
    pub sources: Vec<SourceCoverage>,
    pub containers: Vec<Container>,
}

/// The whole operational-state block for one (account, ocean).
#[derive(Debug, Clone, Default)]
pub struct OperationalState {
    pub connectors: Vec<ConnectorState>,
    /// `source_type`s present in atoms but owned by NO registered connector —
    /// surfaces data drift / forward-compat sources rather than hiding them.
    pub unattributed: Vec<SourceCoverage>,
}

// ── Builder ────────────────────────────────────────────────────────────────────

/// Assemble the operational state for one (account, ocean) from the tables that
/// already hold it. Every sub-query is best-effort: a failure degrades that one
/// field (logged) and never errors the surrounding query.
pub async fn build_operational_state(
    pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
) -> OperationalState {
    // 1. Connection — when each provider was first connected + whether it truly
    //    needs reauth (expired with no refresh token, not just a stale access token).
    let connections = connection_map(pool, account_id).await;

    // 2. Coverage — atom counts + date span per source_type in this ocean.
    let coverage = coverage_rows(pool, account_id, graph_id).await;
    let mut by_source: HashMap<String, SourceCoverage> = HashMap::new();
    for c in &coverage {
        by_source.insert(c.source_type.clone(), c.clone());
    }

    // 3. Attribution — fold connection + coverage + containers into per-connector
    //    state, walking the iterable registry so new connectors appear for free.
    let mut connectors: Vec<ConnectorState> = Vec::new();
    let mut claimed: HashSet<String> = HashSet::new();
    for c in crate::connectors::ALL {
        let id = c.id().to_string();
        let conn = connections.get(&id);
        let mut sources: Vec<SourceCoverage> = Vec::new();
        for st in c.source_types() {
            claimed.insert((*st).to_string());
            if let Some(cov) = by_source.get(*st) {
                sources.push(cov.clone());
            }
        }
        let has_coverage = !sources.is_empty();
        let connected = conn.is_some() || has_coverage;
        // Only pay for the container query when this connector is actually in use.
        let containers = if connected {
            c.describe_containers(pool, account_id, graph_id).await
        } else {
            Vec::new()
        };
        connectors.push(ConnectorState {
            display_name: c.display_name().to_string(),
            connected,
            connected_at: conn.and_then(|(at, _)| *at),
            health: match conn {
                Some((_, true)) => ConnectionHealth::NeedsReauth,
                _ => ConnectionHealth::Connected,
            },
            sources,
            containers,
        });
    }

    // 4. Unattributed — coverage no registered connector claims (drift surfacing).
    let unattributed = coverage
        .into_iter()
        .filter(|c| !claimed.contains(&c.source_type))
        .collect();

    OperationalState {
        connectors,
        unattributed,
    }
}

/// Open a `begin_account_scoped` transaction for one best-effort sub-query
/// (org-tenancy RLS conversion): `oauth_tokens`/`atoms` carry 0083 policies,
/// so every read here must run under the account scope — OPS-4 has switched
/// the app role, so the policies are live, not a future backstop. `None`
/// degrades exactly like a failed query (logged by the
/// caller), preserving this module's "never error the surrounding query"
/// contract.
///
/// **Deliberate deviation from "one scoped transaction per request":**
/// [`build_operational_state`] is a status *aggregator* whose entire contract
/// is that each sub-query degrades INDEPENDENTLY — a broken connection lookup
/// must still leave coverage and containers populated, and vice versa. Folding
/// the three reads into one shared transaction would couple them: the first
/// failure aborts the transaction (SQLSTATE 25P02) and silently empties the
/// rest of the response, changing this endpoint's observable behavior. So each
/// sub-query takes its own short scoped transaction instead. Cheap in
/// practice — this is a low-traffic connector-status surface, not a hot path.
async fn scoped_tx(
    pool: &PgPool,
    account_id: Uuid,
    what: &'static str,
) -> Option<sqlx::Transaction<'static, sqlx::Postgres>> {
    match crate::tenancy::scope::begin_account_scoped(pool, account_id).await {
        Ok(tx) => Some(tx),
        Err(e) => {
            tracing::warn!(error = %e, "operational-state: {what}: scoped tx failed");
            None
        }
    }
}

/// `provider -> (first connected_at, needs_reauth)`, where `needs_reauth` is true
/// only when a token is expired with no refresh token to recover from (see SQL).
async fn connection_map(
    pool: &PgPool,
    account_id: Uuid,
) -> HashMap<String, (Option<DateTime<Utc>>, bool)> {
    let Some(mut tx) = scoped_tx(pool, account_id, "connection query").await else {
        return HashMap::new();
    };
    // `expires_at` is the ACCESS-token expiry, which is short-lived (~1h) for
    // refresh-capable providers like Microsoft — it goes stale within an hour of
    // connecting and is silently renewed from the stored refresh token, so a past
    // `expires_at` alone does NOT mean the user must re-authenticate. Only flag
    // needs-reauth when the token is expired AND there is no refresh token to
    // recover with (`refresh_token IS NULL`) — that's the only case we can be sure
    // requires the user to reconnect. Otherwise we'd cry wolf on every connector's
    // normal hourly access-token rollover.
    // Foundation PR 1.1: account-wide rows only (`owner_user_id IS NULL` —
    // `crate::connectors::TokenOwner::AccountWide`). This status block
    // reports on the ACCOUNT's own connections; a future per-member Outlook
    // token (Train 3) is not an account-wide connection and must not fold
    // into this aggregate.
    let rows = sqlx::query(
        "SELECT provider, MIN(created_at) AS connected_at, \
         BOOL_OR(expires_at IS NOT NULL AND expires_at < now() AND refresh_token IS NULL) \
           AS needs_reauth \
         FROM oauth_tokens WHERE account_id = $1 AND owner_user_id IS NULL GROUP BY provider",
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await;
    tx.rollback().await.ok();

    let mut map = HashMap::new();
    match rows {
        Ok(rows) => {
            for r in rows {
                let provider: String = r.try_get("provider").unwrap_or_default();
                let connected_at: Option<DateTime<Utc>> = r.try_get("connected_at").ok().flatten();
                let needs_reauth: bool = r.try_get("needs_reauth").ok().flatten().unwrap_or(false);
                if !provider.is_empty() {
                    map.insert(provider, (connected_at, needs_reauth));
                }
            }
        }
        Err(e) => tracing::warn!(error = %e, "operational-state: connection query failed"),
    }
    map
}

/// Per-`source_type` coverage for one ocean.
async fn coverage_rows(pool: &PgPool, account_id: Uuid, graph_id: Uuid) -> Vec<SourceCoverage> {
    let Some(mut tx) = scoped_tx(pool, account_id, "coverage query").await else {
        return Vec::new();
    };
    let rows = sqlx::query(
        "SELECT source_type, COUNT(*) AS cnt, MIN(timestamp) AS earliest, \
         MAX(timestamp) AS latest FROM atoms WHERE graph_id = $1 GROUP BY source_type",
    )
    .bind(graph_id)
    .fetch_all(&mut *tx)
    .await;
    tx.rollback().await.ok();

    match rows {
        Ok(rows) => rows
            .into_iter()
            .filter_map(|r| {
                let source_type: String = r.try_get("source_type").ok()?;
                Some(SourceCoverage {
                    source_type,
                    count: r.try_get("cnt").unwrap_or(0),
                    earliest: r.try_get("earliest").ok().flatten(),
                    latest: r.try_get("latest").ok().flatten(),
                })
            })
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "operational-state: coverage query failed");
            Vec::new()
        }
    }
}

/// Default container derivation: distinct channels in `atoms` over the given
/// source types. Connectors whose containers live entirely in atoms (e.g. Slack)
/// use this; connectors with out-of-band container state override
/// [`ConnectorProvider::describe_containers`].
pub(crate) async fn containers_from_atoms(
    pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
    source_types: &[&str],
) -> Vec<Container> {
    if source_types.is_empty() {
        return Vec::new();
    }
    let Some(mut tx) = scoped_tx(pool, account_id, "containers query").await else {
        return Vec::new();
    };
    let types: Vec<String> = source_types.iter().map(|s| s.to_string()).collect();
    let rows = sqlx::query(
        "SELECT channel_id, channel_name, MAX(timestamp) AS last_ts \
         FROM atoms WHERE graph_id = $1 AND source_type = ANY($2) AND channel_id IS NOT NULL \
         GROUP BY channel_id, channel_name ORDER BY last_ts DESC NULLS LAST",
    )
    .bind(graph_id)
    .bind(&types)
    .fetch_all(&mut *tx)
    .await;
    tx.rollback().await.ok();

    match rows {
        Ok(rows) => rows
            .into_iter()
            .map(|r| {
                let id: String = r.try_get("channel_id").unwrap_or_default();
                let name: Option<String> = r.try_get("channel_name").ok().flatten();
                let label = name.filter(|n| !n.is_empty()).unwrap_or(id);
                Container {
                    kind: "channel".to_string(),
                    label,
                    last_ingested: r.try_get("last_ts").ok().flatten(),
                }
            })
            .collect(),
        Err(e) => {
            tracing::warn!(error = %e, "operational-state: containers query failed");
            Vec::new()
        }
    }
}

// ── Rendering ──────────────────────────────────────────────────────────────────

/// Render the operational state into a compact text block for the synthesis
/// prompt. Returns `None` when there is nothing to say (no connections, no
/// coverage) so callers can omit the section entirely.
pub fn render_state_block(state: &OperationalState) -> Option<String> {
    let mut connected_lines: Vec<String> = Vec::new();
    let mut not_connected: Vec<String> = Vec::new();

    for c in &state.connectors {
        if !c.connected {
            not_connected.push(c.display_name.clone());
            continue;
        }
        connected_lines.push(render_connector_line(c));
    }

    let mut lines: Vec<String> = Vec::new();
    lines.extend(connected_lines);
    if !not_connected.is_empty() {
        lines.push(format!("- Not connected: {}.", not_connected.join(", ")));
    }
    if !state.unattributed.is_empty() {
        let parts: Vec<String> = state
            .unattributed
            .iter()
            .map(|s| format!("{} {}", s.source_type, s.count))
            .collect();
        lines.push(format!("- Other sources: {}.", parts.join("; ")));
    }

    // Nothing connected, no coverage, no drift → no block at all.
    let any_connected = state.connectors.iter().any(|c| c.connected);
    if !any_connected && state.unattributed.is_empty() {
        return None;
    }

    Some(lines.join("\n"))
}

fn render_connector_line(c: &ConnectorState) -> String {
    let mut line = format!("- {}: connected", c.display_name);
    if let Some(at) = c.connected_at {
        line.push_str(&format!(" since {}", at.format("%Y-%m-%d")));
    }
    line.push_str(match c.health {
        ConnectionHealth::Connected => " (healthy).",
        ConnectionHealth::NeedsReauth => " (NEEDS REAUTH).",
    });

    if c.sources.is_empty() {
        line.push_str(" No data ingested yet.");
    } else {
        let cov: Vec<String> = c
            .sources
            .iter()
            .map(|s| {
                let span = match (s.earliest, s.latest) {
                    (Some(e), Some(l)) => {
                        format!(", {}→{}", e.format("%Y-%m-%d"), l.format("%Y-%m-%d"))
                    }
                    _ => String::new(),
                };
                format!("{} {}{}", s.source_type, s.count, span)
            })
            .collect();
        line.push(' ');
        line.push_str(&cov.join("; "));
        line.push('.');
    }

    if !c.containers.is_empty() {
        let total = c.containers.len();
        // Most-recently-ingested first, so the capped list shows the live ones.
        let mut conts: Vec<&Container> = c.containers.iter().collect();
        conts.sort_by(|a, b| b.last_ingested.cmp(&a.last_ingested));
        // "Channels" reads wrong when chats/threads are mixed in (Teams/Outlook).
        let noun = if conts.iter().all(|ct| ct.kind == "channel") {
            "Channels"
        } else {
            "Sources"
        };
        let shown: Vec<String> = conts
            .iter()
            .take(MAX_CONTAINERS_RENDERED)
            .map(|ct| ct.label.clone())
            .collect();
        line.push_str(&format!(" {noun}: {}", shown.join(", ")));
        if total > MAX_CONTAINERS_RENDERED {
            line.push_str(&format!(", +{} more", total - MAX_CONTAINERS_RENDERED));
        }
        line.push('.');
    }
    line
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap()
    }

    fn slack_state(connected: bool, health: ConnectionHealth) -> ConnectorState {
        ConnectorState {
            display_name: "Slack".into(),
            connected,
            connected_at: Some(ts(2026, 6, 14)),
            health,
            sources: vec![SourceCoverage {
                source_type: "slack".into(),
                count: 1243,
                earliest: Some(ts(2026, 6, 14)),
                latest: Some(ts(2026, 6, 25)),
            }],
            containers: vec![
                Container {
                    kind: "channel".into(),
                    label: "#eng".into(),
                    last_ingested: Some(ts(2026, 6, 25)),
                },
                Container {
                    kind: "channel".into(),
                    label: "#product".into(),
                    last_ingested: Some(ts(2026, 6, 24)),
                },
            ],
        }
    }

    #[test]
    fn render_healthy_connector_with_channels() {
        let state = OperationalState {
            connectors: vec![slack_state(true, ConnectionHealth::Connected)],
            unattributed: vec![],
        };
        let block = render_state_block(&state).expect("connected → block");
        assert!(block.contains("Slack: connected since 2026-06-14 (healthy)."));
        assert!(block.contains("slack 1243, 2026-06-14→2026-06-25."));
        assert!(block.contains("Channels: #eng, #product."));
    }

    #[test]
    fn render_flags_needs_reauth() {
        let state = OperationalState {
            connectors: vec![slack_state(true, ConnectionHealth::NeedsReauth)],
            unattributed: vec![],
        };
        let block = render_state_block(&state).unwrap();
        assert!(block.contains("(NEEDS REAUTH)."));
    }

    #[test]
    fn render_lists_not_connected_and_returns_none_when_empty() {
        // A registered-but-unused connector lands in "Not connected".
        let mut s = slack_state(false, ConnectionHealth::Connected);
        s.sources.clear();
        s.containers.clear();
        let state = OperationalState {
            connectors: vec![s],
            unattributed: vec![],
        };
        // No connector connected + no drift → nothing to say.
        assert!(render_state_block(&state).is_none());
    }

    #[test]
    fn render_surfaces_unattributed_sources() {
        let state = OperationalState {
            connectors: vec![],
            unattributed: vec![SourceCoverage {
                source_type: "notion".into(),
                count: 7,
                earliest: None,
                latest: None,
            }],
        };
        let block = render_state_block(&state).expect("drift → block");
        assert!(block.contains("Other sources: notion 7."));
    }

    #[test]
    fn render_caps_channels_and_counts_overflow() {
        let mut s = slack_state(true, ConnectionHealth::Connected);
        s.containers = (0..MAX_CONTAINERS_RENDERED + 5)
            .map(|i| Container {
                kind: "channel".into(),
                label: format!("#c{i}"),
                last_ingested: Some(ts(2026, 6, 25)),
            })
            .collect();
        let state = OperationalState {
            connectors: vec![s],
            unattributed: vec![],
        };
        let block = render_state_block(&state).unwrap();
        assert!(block.contains(&format!("+{} more.", 5)));
    }

    #[test]
    fn connected_with_no_ingest_says_so() {
        let mut s = slack_state(true, ConnectionHealth::Connected);
        s.sources.clear();
        s.containers.clear();
        let state = OperationalState {
            connectors: vec![s],
            unattributed: vec![],
        };
        let block = render_state_block(&state).unwrap();
        assert!(block.contains("No data ingested yet."));
    }
}
