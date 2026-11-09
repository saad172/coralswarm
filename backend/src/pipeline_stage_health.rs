//! Per-stage, per-source-type knowledge-pipeline health.
//!
//! Every incident of the week of 2026-10-13 was silent: jobs ended `done`
//! while a stage did nothing. Cloud embeddings were off for five days, standing
//! extraction was switched off, `node_atoms` were never written, duplicate
//! reefs were minted, EntityResolution judged nothing, Jev timed out without
//! binding, and `RegenerateDigest` dead-lettered by the hundred thousand. None
//! of those moved a signal anyone watched. This module measures each stage on
//! its own and turns "upstream is flowing but this stage is not" into an
//! alert.
//!
//! Stages, per source type (cohort = captured in `[now - H, now - grace)`):
//!
//! * capture: atoms and current source versions;
//! * embedding: `embedding_next` written, `skipped:` / `blocked:` / `held:`
//!   stamps, exhausted retries, the oldest unembedded atom;
//! * extraction: `source_versions.extraction_status`, minus waits that are a
//!   live decision (standing run paused, admission excluded/unavailable);
//! * linking: done versions without `source_version_chunks` rows, and done
//!   versions none of whose chunk atoms reached a reef through `node_atoms`;
//! * reefs: links to reefs minted in the window vs reefs that already
//!   existed (mint vs bind), and reefs minted in the window whose
//!   `(graph, type, lower(title))` already named an OLDER reef (duplicates
//!   against existing reefs, not just within the new ones);
//! * titles: fallback share, and whether any non-fallback title landed.
//!
//! Per plane: relationship edges, ER judgements and merges, hourly journal
//! rollups, Jev binding calls (ok / withheld / errored, plus calls still
//! pending past their timeout), dead-lettered jobs per kind, standing run
//! pause states, managed spend against the daily cap per purpose, and managed
//! reservations held past [`Thresholds::hold_max_minutes`].
//!
//! Every query is bounded by a time window on an indexed column (tenant
//! 0172-0176 / control 0339-0343 add the five that were missing) and by the
//! caller's graph ids; nothing fans out per row. The queries take
//! `&mut PgConnection` so the caller supplies the scope:
//! `workers::pipeline_stage_health` opens one account-scoped transaction per
//! account on the consumer plane and one tenant-system transaction on an org
//! tenant; `bin/pipeline_stage_health` opens one `READ ONLY` transaction.
//! [`harden`] makes either transaction read-only with a statement timeout.
//!
//! [`evaluate`] is pure: the worker pages on its findings, and the admin route
//! re-evaluates stored reports with the current thresholds.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use uuid::Uuid;

/// The Jev extraction-binding usage task (`decision::openrouter`).
pub const JEV_TASK: &str = crate::decision::openrouter::EXTRACTION_BINDING_TASK;
/// A Jev binding call is abandoned when its pending usage row outlives the
/// 120 s whole-call budget plus this slack (the usage row is only finalized
/// when the call returns; a timed-out call leaves it pending until the
/// orphan sweeper reaps it 15 minutes later).
pub const JEV_TIMEOUT_SECS: i64 = 150;
/// `embedding::MAX_EMBED_ATTEMPTS`: an atom past this is not retried.
pub const MAX_EMBED_ATTEMPTS: i32 = crate::embedding::MAX_EMBED_ATTEMPTS;
/// Statement timeout applied by [`harden`].
pub const STATEMENT_TIMEOUT: &str = "30s";

// ── Thresholds ───────────────────────────────────────────────────────────────

/// Alert thresholds, each from `STAGE_HEALTH_*` env with a default. A window
/// of `0` hours disables that rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Thresholds {
    /// Ignore captures younger than this: the next stage has not had a turn.
    pub grace_minutes: i64,
    /// A cohort smaller than this never alerts.
    pub min_flow: i64,
    /// `pipeline_embed_stalled`: captures, none embedded or skipped.
    pub embed_stall_hours: i64,
    /// `pipeline_extraction_stalled`: unheld waits, none done.
    pub extraction_stall_hours: i64,
    /// `pipeline_linking_stalled`: done versions, no chunk rows or no links.
    pub link_stall_hours: i64,
    /// `pipeline_reef_duplicates` and `pipeline_er_stalled`.
    pub reef_window_hours: i64,
    pub dup_rate_max: f64,
    pub dup_min_minted: i64,
    /// `pipeline_edges_stalled`.
    pub edge_stall_hours: i64,
    /// `pipeline_journal_stalled`.
    pub journal_stall_hours: i64,
    /// `pipeline_titles_stalled`: no non-fallback title at all.
    pub title_window_hours: i64,
    pub title_min_atoms: i64,
    /// `pipeline_jev_degraded`.
    pub jev_window_hours: i64,
    pub jev_min_calls: i64,
    pub jev_min_success: f64,
    /// `pipeline_jobs_dead`: dead letters per kind.
    pub dead_jobs_hours: i64,
    pub dead_jobs_max: i64,
    /// `pipeline_spend_at_cap`: share of the daily cap reserved or charged.
    pub spend_cap_fraction: f64,
    /// `pipeline_hold_stuck`: managed reservations older than this.
    pub hold_max_minutes: i64,
    /// The operator table's long window.
    pub long_hours: i64,
    /// A stage that never flowed in this window is not expected to flow:
    /// extraction, linking, edges, journal and titles only alert for a source
    /// type (or plane) that did that work at least once in it.
    pub baseline_hours: i64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            grace_minutes: 15,
            min_flow: 3,
            embed_stall_hours: 2,
            extraction_stall_hours: 6,
            link_stall_hours: 6,
            reef_window_hours: 24,
            dup_rate_max: 0.25,
            dup_min_minted: 10,
            edge_stall_hours: 24,
            journal_stall_hours: 6,
            title_window_hours: 24,
            title_min_atoms: 20,
            jev_window_hours: 6,
            jev_min_calls: 5,
            jev_min_success: 0.5,
            dead_jobs_hours: 2,
            dead_jobs_max: 100,
            spend_cap_fraction: 0.95,
            hold_max_minutes: 30,
            long_hours: 24,
            baseline_hours: 168,
        }
    }
}

fn int_env(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(default)
}

fn ratio_env(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(default)
}

impl Thresholds {
    pub fn from_env() -> Self {
        let d = Self::default();
        Self {
            grace_minutes: int_env("STAGE_HEALTH_GRACE_MINUTES", d.grace_minutes),
            min_flow: int_env("STAGE_HEALTH_MIN_FLOW", d.min_flow).max(1),
            embed_stall_hours: int_env("STAGE_HEALTH_EMBED_STALL_HOURS", d.embed_stall_hours),
            extraction_stall_hours: int_env("STAGE_HEALTH_EXTRACTION_STALL_HOURS", d.extraction_stall_hours),
            link_stall_hours: int_env("STAGE_HEALTH_LINK_STALL_HOURS", d.link_stall_hours),
            reef_window_hours: int_env("STAGE_HEALTH_REEF_WINDOW_HOURS", d.reef_window_hours),
            dup_rate_max: ratio_env("STAGE_HEALTH_DUP_RATE_MAX", d.dup_rate_max),
            dup_min_minted: int_env("STAGE_HEALTH_DUP_MIN_MINTED", d.dup_min_minted).max(1),
            edge_stall_hours: int_env("STAGE_HEALTH_EDGE_STALL_HOURS", d.edge_stall_hours),
            journal_stall_hours: int_env("STAGE_HEALTH_JOURNAL_STALL_HOURS", d.journal_stall_hours),
            title_window_hours: int_env("STAGE_HEALTH_TITLE_WINDOW_HOURS", d.title_window_hours),
            title_min_atoms: int_env("STAGE_HEALTH_TITLE_MIN_ATOMS", d.title_min_atoms).max(1),
            jev_window_hours: int_env("STAGE_HEALTH_JEV_WINDOW_HOURS", d.jev_window_hours),
            jev_min_calls: int_env("STAGE_HEALTH_JEV_MIN_CALLS", d.jev_min_calls).max(1),
            jev_min_success: ratio_env("STAGE_HEALTH_JEV_MIN_SUCCESS", d.jev_min_success),
            dead_jobs_hours: int_env("STAGE_HEALTH_DEAD_JOBS_HOURS", d.dead_jobs_hours),
            dead_jobs_max: int_env("STAGE_HEALTH_DEAD_JOBS_MAX", d.dead_jobs_max),
            spend_cap_fraction: ratio_env("STAGE_HEALTH_SPEND_CAP_FRACTION", d.spend_cap_fraction),
            hold_max_minutes: int_env("STAGE_HEALTH_HOLD_MAX_MINUTES", d.hold_max_minutes),
            long_hours: int_env("STAGE_HEALTH_LONG_HOURS", d.long_hours).max(1),
            baseline_hours: int_env("STAGE_HEALTH_BASELINE_HOURS", d.baseline_hours).max(1),
        }
    }

    /// Every window the rules and the operator table read, ascending.
    pub fn windows(&self) -> Vec<i64> {
        let mut hours = vec![
            self.embed_stall_hours,
            self.extraction_stall_hours,
            self.link_stall_hours,
            self.reef_window_hours,
            self.edge_stall_hours,
            self.journal_stall_hours,
            self.title_window_hours,
            self.jev_window_hours,
            self.dead_jobs_hours,
            self.long_hours,
            self.baseline_hours,
        ];
        hours.retain(|h| *h > 0);
        hours.sort_unstable();
        hours.dedup();
        hours
    }
}

// ── Report model ─────────────────────────────────────────────────────────────

/// One source type's stages over one window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SourceStages {
    pub source_type: String,
    // capture + embedding (atom cohort)
    pub atoms: i64,
    pub embedded: i64,
    pub embed_skipped: i64,
    pub embed_blocked: i64,
    pub embed_held: i64,
    pub embed_failed: i64,
    pub oldest_unembedded_at: Option<DateTime<Utc>>,
    // titles
    pub fallback_titles: i64,
    pub real_titles: i64,
    // reef links of the cohort's atoms
    pub linked_atoms: i64,
    pub links_to_new: i64,
    pub links_to_existing: i64,
    // extraction (current, permitted versions)
    pub versions: i64,
    pub extraction_done: i64,
    pub extraction_failed: i64,
    pub extraction_blocked: i64,
    pub extraction_waiting: i64,
    pub extraction_held: i64,
    pub oldest_waiting_at: Option<DateTime<Utc>>,
    pub done_without_chunks: i64,
    pub done_linked: i64,
    // reefs minted in the window, attributed to their earliest atom's type
    pub reefs_minted: i64,
    pub reefs_minted_duplicate: i64,
}

impl SourceStages {
    /// Atoms still waiting for a vector with nothing deciding the wait.
    pub fn embed_pending(&self) -> i64 {
        (self.atoms
            - self.embedded
            - self.embed_skipped
            - self.embed_blocked
            - self.embed_held
            - self.embed_failed)
            .max(0)
    }

    fn absorb(&mut self, o: &SourceStages) {
        self.atoms += o.atoms;
        self.embedded += o.embedded;
        self.embed_skipped += o.embed_skipped;
        self.embed_blocked += o.embed_blocked;
        self.embed_held += o.embed_held;
        self.embed_failed += o.embed_failed;
        self.oldest_unembedded_at = min_opt(self.oldest_unembedded_at, o.oldest_unembedded_at);
        self.fallback_titles += o.fallback_titles;
        self.real_titles += o.real_titles;
        self.linked_atoms += o.linked_atoms;
        self.links_to_new += o.links_to_new;
        self.links_to_existing += o.links_to_existing;
        self.versions += o.versions;
        self.extraction_done += o.extraction_done;
        self.extraction_failed += o.extraction_failed;
        self.extraction_blocked += o.extraction_blocked;
        self.extraction_waiting += o.extraction_waiting;
        self.extraction_held += o.extraction_held;
        self.oldest_waiting_at = min_opt(self.oldest_waiting_at, o.oldest_waiting_at);
        self.done_without_chunks += o.done_without_chunks;
        self.done_linked += o.done_linked;
        self.reefs_minted += o.reefs_minted;
        self.reefs_minted_duplicate += o.reefs_minted_duplicate;
    }
}

fn min_opt(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Plane-level stages over one window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GraphStages {
    pub relationship_edges: i64,
    pub er_judgements: i64,
    pub er_positive: i64,
    pub er_unsure: i64,
    pub er_merges: i64,
    pub journal_hourly_rollups: i64,
    pub jev_ok: i64,
    pub jev_withheld: i64,
    pub jev_errored: i64,
}

impl GraphStages {
    fn absorb(&mut self, o: &GraphStages) {
        self.relationship_edges += o.relationship_edges;
        self.er_judgements += o.er_judgements;
        self.er_positive += o.er_positive;
        self.er_unsure += o.er_unsure;
        self.er_merges += o.er_merges;
        self.journal_hourly_rollups += o.journal_hourly_rollups;
        self.jev_ok += o.jev_ok;
        self.jev_withheld += o.jev_withheld;
        self.jev_errored += o.jev_errored;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct KindCount {
    pub kind: String,
    pub count: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WindowStages {
    pub hours: i64,
    pub sources: Vec<SourceStages>,
    pub graph: GraphStages,
    /// Dead-lettered jobs per kind (top 20), plane-wide.
    pub dead_jobs: Vec<KindCount>,
}

impl WindowStages {
    pub fn source(&self, source_type: &str) -> Option<&SourceStages> {
        self.sources.iter().find(|s| s.source_type == source_type)
    }
    fn totals(&self) -> SourceStages {
        let mut total = SourceStages { source_type: "*".into(), ..Default::default() };
        for s in &self.sources {
            total.absorb(s);
        }
        total
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct StandingRun {
    pub extraction_version: String,
    pub status: String,
    pub pause_state: Option<String>,
    pub runs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct PurposeSpend {
    pub account_id: Uuid,
    pub purpose: String,
    pub limit_micro_usd: i64,
    pub reserved_micro_usd: i64,
    pub charged_micro_usd: i64,
}

impl PurposeSpend {
    pub fn used_fraction(&self) -> f64 {
        if self.limit_micro_usd <= 0 {
            return if self.reserved_micro_usd + self.charged_micro_usd > 0 { f64::INFINITY } else { 0.0 };
        }
        (self.reserved_micro_usd + self.charged_micro_usd) as f64 / self.limit_micro_usd as f64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct StuckHolds {
    /// `managed_attempt_receipts.purpose`, or `credit_reservation` for the
    /// control-plane `managed_credit_reservations` hold.
    pub purpose: String,
    pub holds: i64,
    pub oldest_at: Option<DateTime<Utc>>,
}

/// Process switches that silently turn a stage off.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Switches {
    pub standing_processing_enabled: bool,
    pub enrichment_processing_enabled: bool,
    pub managed_processing_disabled: bool,
    pub node_atoms_bridge_enabled: bool,
    pub jev_binding_enabled: bool,
}

impl Switches {
    pub fn current() -> Self {
        Self {
            standing_processing_enabled: crate::managed::standing::execution::rollout_enabled(),
            enrichment_processing_enabled: crate::managed::enrichment::enabled(),
            managed_processing_disabled: crate::managed::Caps::from_env().disabled,
            node_atoms_bridge_enabled: crate::node_atoms_bridge::enabled(),
            jev_binding_enabled: crate::workers::source_atoms::v2::jev_binding_enabled(),
        }
    }
}

/// Point-in-time state: not windowed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub standing_runs: Vec<StandingRun>,
    /// Today's (UTC) managed budget rows.
    pub spend: Vec<PurposeSpend>,
    /// Reservations older than the hold threshold.
    pub stuck_holds: Vec<StuckHolds>,
    /// Jev binding calls whose usage row is still pending past the timeout.
    pub jev_timed_out: i64,
}

impl Snapshot {
    /// Adds a hold count, merging by purpose.
    pub fn add_hold(&mut self, hold: StuckHolds) {
        match self.stuck_holds.iter_mut().find(|h| h.purpose == hold.purpose) {
            Some(h) => {
                h.holds += hold.holds;
                h.oldest_at = min_opt(h.oldest_at, hold.oldest_at);
            }
            None => self.stuck_holds.push(hold),
        }
    }

    fn absorb(&mut self, o: Snapshot) {
        self.standing_runs.extend(o.standing_runs);
        self.spend.extend(o.spend);
        for hold in o.stuck_holds {
            self.add_hold(hold);
        }
        self.jev_timed_out += o.jev_timed_out;
    }
}

/// One plane's measurement, as stored in the control table
/// `pipeline_stage_health`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StageReport {
    /// `'__consumer__'` or the Clerk org id.
    pub tenant: String,
    pub measured_at: DateTime<Utc>,
    pub grace_minutes: i64,
    /// Accounts measured, and accounts skipped because they switched
    /// processing off (their stages are not expected to flow).
    pub accounts_measured: i64,
    pub accounts_processing_off: i64,
    /// The measuring process's switches; `None` from the operator bin, which
    /// cannot see the serving process's env.
    pub switches: Option<Switches>,
    pub windows: Vec<WindowStages>,
    pub snapshot: Snapshot,
}

impl StageReport {
    pub fn window(&self, hours: i64) -> Option<&WindowStages> {
        self.windows.iter().find(|w| w.hours == hours)
    }
}

/// Accumulates one plane's per-account measurements.
#[derive(Debug, Default)]
pub struct PlaneAccumulator {
    windows: BTreeMap<i64, (BTreeMap<String, SourceStages>, GraphStages)>,
    snapshot: Snapshot,
    pub accounts_measured: i64,
}

impl PlaneAccumulator {
    pub fn add(&mut self, measured: Measured) {
        self.accounts_measured += 1;
        for w in measured.windows {
            let (sources, graph) = self.windows.entry(w.hours).or_default();
            for s in w.sources {
                sources
                    .entry(s.source_type.clone())
                    .or_insert_with(|| SourceStages { source_type: s.source_type.clone(), ..Default::default() })
                    .absorb(&s);
            }
            graph.absorb(&w.graph);
        }
        self.snapshot.absorb(measured.snapshot);
    }

    pub fn finish(
        self,
        tenant: String,
        measured_at: DateTime<Utc>,
        th: &Thresholds,
        windows: &[i64],
        dead_jobs: Vec<(i64, Vec<KindCount>)>,
        accounts_processing_off: i64,
        switches: Option<Switches>,
    ) -> StageReport {
        let mut windows: Vec<WindowStages> = windows
            .iter()
            .copied()
            .map(|hours| {
                let (sources, graph) = self.windows.get(&hours).cloned().unwrap_or_default();
                let mut sources: Vec<SourceStages> = sources.into_values().collect();
                sources.sort_by(|a, b| b.atoms.cmp(&a.atoms).then_with(|| a.source_type.cmp(&b.source_type)));
                WindowStages { hours, sources, graph, dead_jobs: Vec::new() }
            })
            .collect();
        for (hours, kinds) in dead_jobs {
            if let Some(w) = windows.iter_mut().find(|w| w.hours == hours) {
                w.dead_jobs = kinds;
            }
        }
        StageReport {
            tenant,
            measured_at,
            grace_minutes: th.grace_minutes,
            accounts_measured: self.accounts_measured,
            accounts_processing_off,
            switches,
            windows,
            snapshot: self.snapshot,
        }
    }
}

/// One account's (or one org tenant's) raw measurement.
#[derive(Debug, Default)]
pub struct Measured {
    pub windows: Vec<WindowStages>,
    pub snapshot: Snapshot,
}

// ── Queries ──────────────────────────────────────────────────────────────────

/// Make the caller's transaction read-only with a bounded statement time.
/// Both are transaction-local (`SET LOCAL` semantics), so they never leak
/// onto a pooled server connection.
pub async fn harden(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("SET TRANSACTION READ ONLY").execute(&mut *conn).await?;
    sqlx::query("SELECT set_config('statement_timeout', $1, true)")
        .bind(STATEMENT_TIMEOUT)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Atom cohort per source type. `$1` graphs, `$2` since, `$3` until, `$4`
/// max embed attempts. Index: `idx_atoms_graph_created`; the lateral reads
/// `idx_node_atoms_atom_id` and `nodes_pkey`.
pub const ATOM_STAGES_SQL: &str = "\
SELECT a.source_type, \
  count(*)::int8 AS atoms, \
  count(*) FILTER (WHERE a.embedding_next IS NOT NULL)::int8 AS embedded, \
  count(*) FILTER (WHERE a.embedding_next IS NULL AND a.embed_model_next LIKE 'skipped:%')::int8 AS embed_skipped, \
  count(*) FILTER (WHERE a.embedding_next IS NULL AND a.embed_model_next LIKE 'blocked:%')::int8 AS embed_blocked, \
  count(*) FILTER (WHERE a.embedding_next IS NULL AND a.embed_model_next LIKE 'held:%')::int8 AS embed_held, \
  count(*) FILTER (WHERE a.embedding_next IS NULL AND a.embed_failed_at IS NOT NULL AND a.embed_attempts >= $4 \
    AND COALESCE(a.embed_model_next,'') NOT LIKE ALL (ARRAY['skipped:%','blocked:%','held:%']))::int8 AS embed_failed, \
  min(a.created_at) FILTER (WHERE a.embedding_next IS NULL \
    AND COALESCE(a.embed_model_next,'') NOT LIKE ALL (ARRAY['skipped:%','blocked:%','held:%'])) AS oldest_unembedded_at, \
  count(*) FILTER (WHERE a.title_kind = 'fallback')::int8 AS fallback_titles, \
  count(*) FILTER (WHERE a.title IS NOT NULL AND a.title_kind IS DISTINCT FROM 'fallback')::int8 AS real_titles, \
  count(*) FILTER (WHERE l.links > 0)::int8 AS linked_atoms, \
  COALESCE(sum(l.to_new),0)::int8 AS links_to_new, \
  COALESCE(sum(l.links - l.to_new),0)::int8 AS links_to_existing \
FROM atoms a \
LEFT JOIN LATERAL ( \
  SELECT count(*) AS links, count(*) FILTER (WHERE n.created_at >= $2) AS to_new \
  FROM node_atoms na JOIN nodes n ON n.node_id = na.node_id WHERE na.atom_id = a.atom_id \
) l ON true \
WHERE a.graph_id = ANY($1) AND a.created_at >= $2 AND a.created_at < $3 \
GROUP BY a.source_type";

/// Current, permitted source versions per source type. `$1` graphs, `$2`
/// since, `$3` until. Index: `source_versions_graph_captured` (tenant 0172 /
/// control 0339). A wait is held, not stalled, when the graph's live standing
/// run for that extraction version is paused, or the admission receipt says
/// the source was excluded or became unavailable.
pub const VERSION_STAGES_SQL: &str = "\
SELECT v.source_type, \
  count(*)::int8 AS versions, \
  count(*) FILTER (WHERE v.extraction_status = 'done')::int8 AS extraction_done, \
  count(*) FILTER (WHERE v.extraction_status = 'failed')::int8 AS extraction_failed, \
  count(*) FILTER (WHERE v.extraction_status = 'blocked')::int8 AS extraction_blocked, \
  count(*) FILTER (WHERE v.extraction_status IN ('not_started','pending','running') AND NOT h.held)::int8 AS extraction_waiting, \
  count(*) FILTER (WHERE v.extraction_status IN ('not_started','pending','running') AND h.held)::int8 AS extraction_held, \
  min(v.captured_at) FILTER (WHERE v.extraction_status IN ('not_started','pending','running') AND NOT h.held) AS oldest_waiting_at, \
  count(*) FILTER (WHERE v.extraction_status = 'done' AND NOT EXISTS ( \
    SELECT 1 FROM source_version_chunks c WHERE c.source_version_id = v.version_id))::int8 AS done_without_chunks, \
  count(*) FILTER (WHERE v.extraction_status = 'done' AND EXISTS ( \
    SELECT 1 FROM source_version_chunks c JOIN node_atoms na ON na.atom_id = c.atom_id \
    WHERE c.source_version_id = v.version_id))::int8 AS done_linked \
FROM source_versions v \
JOIN source_items si ON si.source_item_id = v.source_item_id AND si.graph_id = v.graph_id \
  AND si.current_version_id = v.version_id AND si.access_state = 'permitted' \
CROSS JOIN LATERAL (SELECT ( \
  EXISTS (SELECT 1 FROM corpus_enrichment_runs r WHERE r.graph_id = v.graph_id AND r.run_kind = 'standing' \
    AND r.retired_at IS NULL AND r.extraction_version = v.extraction_version \
    AND r.standing_pause_state IS DISTINCT FROM 'none') \
  OR EXISTS (SELECT 1 FROM corpus_capture_admissions ca WHERE ca.source_version_id = v.version_id \
    AND ca.state IN ('excluded','unavailable')) \
) AS held) h \
WHERE v.graph_id = ANY($1) AND v.captured_at >= $2 AND v.captured_at < $3 AND NOT v.is_legacy_baseline \
GROUP BY v.source_type";

/// Reefs minted in the window, attributed to the source type of their
/// earliest linked atom (`''` when they have none), and how many of them
/// repeat the `(graph, type, lower(title))` of an OLDER reef. Index:
/// `nodes_graph_created` (tenant 0173 / control 0340) for the window,
/// `idx_nodes_graph_type_lower_title` for the duplicate probe.
pub const MINTED_REEFS_SQL: &str = "\
SELECT COALESCE(src.source_type, '') AS source_type, \
  count(*)::int8 AS reefs_minted, \
  count(*) FILTER (WHERE EXISTS ( \
    SELECT 1 FROM nodes o WHERE o.graph_id = n.graph_id AND o.type = n.type \
      AND lower(o.title) = lower(n.title) AND o.node_id <> n.node_id AND o.created_at < n.created_at \
  ))::int8 AS reefs_minted_duplicate \
FROM nodes n \
LEFT JOIN LATERAL ( \
  SELECT a.source_type FROM node_atoms na JOIN atoms a ON a.atom_id = na.atom_id \
  WHERE na.node_id = n.node_id ORDER BY a.created_at, a.atom_id LIMIT 1 \
) src ON true \
WHERE n.graph_id = ANY($1) AND n.created_at >= $2 AND n.created_at < $3 \
GROUP BY 1";

/// Plane-level stages. `$1` graphs, `$2` since, `$3` until, `$4` accounts,
/// `$5` the Jev binding task. Indexes: `relationship_edge_graph_created`
/// (tenant 0174 / control 0341), `reef_judgement_graph_created` (0175 /
/// 0342), `idx_node_merges_graph_active`, `journal_rollups_tier_read_idx`,
/// `idx_llm_usage_account_created` (managed calls write `llm_usage` with a
/// NULL `account_id`, so those rows are matched by graph under the same
/// index's `account_id IS NULL` range).
pub const GRAPH_STAGES_SQL: &str = "\
WITH j AS ( \
  SELECT verdict FROM reef_judgement \
  WHERE graph_id = ANY($1) AND created_at >= $2 AND created_at < $3 \
), u AS ( \
  SELECT task FROM llm_usage \
  WHERE created_at >= $2 AND created_at < $3 \
    AND (account_id = ANY($4) OR (account_id IS NULL AND graph_id = ANY($1))) \
    AND task IN ($5, $5 || '_withheld', $5 || '_errored') \
) \
SELECT \
  (SELECT count(*) FROM relationship_edge e WHERE e.graph_id = ANY($1) AND e.created_at >= $2 AND e.created_at < $3)::int8 AS relationship_edges, \
  (SELECT count(*) FROM j)::int8 AS er_judgements, \
  (SELECT count(*) FROM j WHERE verdict = 'positive')::int8 AS er_positive, \
  (SELECT count(*) FROM j WHERE verdict = 'unsure')::int8 AS er_unsure, \
  (SELECT count(*) FROM node_merges m WHERE m.graph_id = ANY($1) AND m.undone_at IS NULL \
     AND m.created_at >= $2 AND m.created_at < $3)::int8 AS er_merges, \
  (SELECT count(*) FROM journal_rollups r WHERE r.granularity = 'hour' AND r.period_start >= $2 \
     AND r.period_start < $3 AND r.graph_id = ANY($1))::int8 AS journal_hourly_rollups, \
  (SELECT count(*) FROM u WHERE task = $5)::int8 AS jev_ok, \
  (SELECT count(*) FROM u WHERE task = $5 || '_withheld')::int8 AS jev_withheld, \
  (SELECT count(*) FROM u WHERE task = $5 || '_errored')::int8 AS jev_errored";

/// Dead letters per kind, plane-wide (`jobs` carries no RLS). `$1` since,
/// `$2` until. Index: `jobs_dead_finished` (tenant 0176 / control 0343).
pub const DEAD_JOBS_SQL: &str = "\
SELECT kind, count(*)::int8 AS count FROM jobs \
WHERE status = 'dead' AND finished_at >= $1 AND finished_at < $2 \
GROUP BY kind ORDER BY 2 DESC, 1 LIMIT 20";

const STANDING_RUNS_SQL: &str = "\
SELECT extraction_version, status, standing_pause_state AS pause_state, count(*)::int8 AS runs \
FROM corpus_enrichment_runs \
WHERE graph_id = ANY($1) AND run_kind = 'standing' AND retired_at IS NULL \
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3";

/// Today's (UTC, per `$2`) budget rows. Primary key lookup.
const SPEND_SQL: &str = "\
SELECT account_id, purpose, limit_micro_usd, reserved_micro_usd, charged_micro_usd \
FROM managed_budget_periods \
WHERE account_id = ANY($1) AND period_start = ($2::timestamptz AT TIME ZONE 'UTC')::date \
ORDER BY account_id, purpose";

/// Open attempt reservations older than `$2`. Index:
/// `managed_attempt_receipts_open` (partial on `state = 'reserved'`).
const STUCK_RECEIPTS_SQL: &str = "\
SELECT purpose, count(*)::int8 AS holds, min(created_at) AS oldest_at \
FROM managed_attempt_receipts \
WHERE state = 'reserved' AND created_at < $2 AND account_id = ANY($1) \
GROUP BY purpose ORDER BY purpose";

/// Jev calls still pending past the timeout. Index:
/// `idx_llm_usage_pending_kind_started_at`.
const JEV_TIMED_OUT_SQL: &str = "\
SELECT count(*)::int8 FROM llm_usage_pending \
WHERE kind = 'sync' AND started_at < $2 AND task = $3 \
  AND (account_id = ANY($1) OR (account_id IS NULL AND graph_id = ANY($4)))";

/// Control-plane credit holds older than `$2` for one account on plane `$3`
/// (`'consumer'` or `'org:<clerk org id>'`). Index:
/// `managed_credit_reservations_outstanding` (partial on `state = 'held'`).
pub const CREDIT_HOLDS_SQL: &str = "\
SELECT 'credit_reservation'::text AS purpose, count(*)::int8 AS holds, min(created_at) AS oldest_at \
FROM managed_credit_reservations \
WHERE account_id = $1 AND state = 'held' AND created_at < $2 AND plane = $3";

#[derive(sqlx::FromRow)]
struct AtomRow {
    source_type: String,
    atoms: i64,
    embedded: i64,
    embed_skipped: i64,
    embed_blocked: i64,
    embed_held: i64,
    embed_failed: i64,
    oldest_unembedded_at: Option<DateTime<Utc>>,
    fallback_titles: i64,
    real_titles: i64,
    linked_atoms: i64,
    links_to_new: i64,
    links_to_existing: i64,
}

#[derive(sqlx::FromRow)]
struct VersionRow {
    source_type: String,
    versions: i64,
    extraction_done: i64,
    extraction_failed: i64,
    extraction_blocked: i64,
    extraction_waiting: i64,
    extraction_held: i64,
    oldest_waiting_at: Option<DateTime<Utc>>,
    done_without_chunks: i64,
    done_linked: i64,
}

#[derive(sqlx::FromRow)]
struct MintedRow {
    source_type: String,
    reefs_minted: i64,
    reefs_minted_duplicate: i64,
}

#[derive(sqlx::FromRow)]
struct GraphRow {
    relationship_edges: i64,
    er_judgements: i64,
    er_positive: i64,
    er_unsure: i64,
    er_merges: i64,
    journal_hourly_rollups: i64,
    jev_ok: i64,
    jev_withheld: i64,
    jev_errored: i64,
}

/// The half-open cohort `[now - hours, now - grace)`.
pub fn cohort(now: DateTime<Utc>, hours: i64, grace_minutes: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    (now - Duration::hours(hours), now - Duration::minutes(grace_minutes))
}

/// One window's stages for `graph_ids` (owned by `account_ids`).
pub async fn measure_window(
    conn: &mut PgConnection,
    graph_ids: &[Uuid],
    account_ids: &[Uuid],
    now: DateTime<Utc>,
    hours: i64,
    grace_minutes: i64,
) -> Result<WindowStages, sqlx::Error> {
    let (since, until) = cohort(now, hours, grace_minutes);
    let mut by_type: BTreeMap<String, SourceStages> = BTreeMap::new();
    let atoms: Vec<AtomRow> = sqlx::query_as(ATOM_STAGES_SQL)
        .bind(graph_ids)
        .bind(since)
        .bind(until)
        .bind(MAX_EMBED_ATTEMPTS)
        .fetch_all(&mut *conn)
        .await?;
    for r in atoms {
        let s = slot(&mut by_type, &r.source_type);
        s.atoms = r.atoms;
        s.embedded = r.embedded;
        s.embed_skipped = r.embed_skipped;
        s.embed_blocked = r.embed_blocked;
        s.embed_held = r.embed_held;
        s.embed_failed = r.embed_failed;
        s.oldest_unembedded_at = r.oldest_unembedded_at;
        s.fallback_titles = r.fallback_titles;
        s.real_titles = r.real_titles;
        s.linked_atoms = r.linked_atoms;
        s.links_to_new = r.links_to_new;
        s.links_to_existing = r.links_to_existing;
    }
    let versions: Vec<VersionRow> = sqlx::query_as(VERSION_STAGES_SQL)
        .bind(graph_ids)
        .bind(since)
        .bind(until)
        .fetch_all(&mut *conn)
        .await?;
    for r in versions {
        let s = slot(&mut by_type, &r.source_type);
        s.versions = r.versions;
        s.extraction_done = r.extraction_done;
        s.extraction_failed = r.extraction_failed;
        s.extraction_blocked = r.extraction_blocked;
        s.extraction_waiting = r.extraction_waiting;
        s.extraction_held = r.extraction_held;
        s.oldest_waiting_at = r.oldest_waiting_at;
        s.done_without_chunks = r.done_without_chunks;
        s.done_linked = r.done_linked;
    }
    let minted: Vec<MintedRow> = sqlx::query_as(MINTED_REEFS_SQL)
        .bind(graph_ids)
        .bind(since)
        .bind(until)
        .fetch_all(&mut *conn)
        .await?;
    for r in minted {
        let key = if r.source_type.is_empty() { "(no atoms)" } else { r.source_type.as_str() };
        let s = slot(&mut by_type, key);
        s.reefs_minted = r.reefs_minted;
        s.reefs_minted_duplicate = r.reefs_minted_duplicate;
    }
    let g: GraphRow = sqlx::query_as(GRAPH_STAGES_SQL)
        .bind(graph_ids)
        .bind(since)
        .bind(until)
        .bind(account_ids)
        .bind(JEV_TASK)
        .fetch_one(&mut *conn)
        .await?;
    Ok(WindowStages {
        hours,
        sources: by_type.into_values().collect(),
        graph: GraphStages {
            relationship_edges: g.relationship_edges,
            er_judgements: g.er_judgements,
            er_positive: g.er_positive,
            er_unsure: g.er_unsure,
            er_merges: g.er_merges,
            journal_hourly_rollups: g.journal_hourly_rollups,
            jev_ok: g.jev_ok,
            jev_withheld: g.jev_withheld,
            jev_errored: g.jev_errored,
        },
        dead_jobs: Vec::new(),
    })
}

fn slot<'a>(m: &'a mut BTreeMap<String, SourceStages>, t: &str) -> &'a mut SourceStages {
    m.entry(t.to_string())
        .or_insert_with(|| SourceStages { source_type: t.to_string(), ..Default::default() })
}

/// Point-in-time state for `graph_ids` / `account_ids` on the content plane.
pub async fn measure_snapshot(
    conn: &mut PgConnection,
    graph_ids: &[Uuid],
    account_ids: &[Uuid],
    now: DateTime<Utc>,
    th: &Thresholds,
) -> Result<Snapshot, sqlx::Error> {
    let standing_runs = sqlx::query_as(STANDING_RUNS_SQL).bind(graph_ids).fetch_all(&mut *conn).await?;
    let spend = sqlx::query_as(SPEND_SQL).bind(account_ids).bind(now).fetch_all(&mut *conn).await?;
    let stuck_holds = sqlx::query_as(STUCK_RECEIPTS_SQL)
        .bind(account_ids)
        .bind(now - Duration::minutes(th.hold_max_minutes))
        .fetch_all(&mut *conn)
        .await?;
    let jev_timed_out: i64 = sqlx::query_scalar(JEV_TIMED_OUT_SQL)
        .bind(account_ids)
        .bind(now - Duration::seconds(JEV_TIMEOUT_SECS))
        .bind(JEV_TASK)
        .bind(graph_ids)
        .fetch_one(&mut *conn)
        .await?;
    Ok(Snapshot { standing_runs, spend, stuck_holds, jev_timed_out })
}

/// `windows` plus the snapshot for one scope.
pub async fn measure_scope(
    conn: &mut PgConnection,
    graph_ids: &[Uuid],
    account_ids: &[Uuid],
    now: DateTime<Utc>,
    windows_hours: &[i64],
    th: &Thresholds,
) -> Result<Measured, sqlx::Error> {
    let mut windows = Vec::new();
    for &hours in windows_hours {
        windows.push(measure_window(conn, graph_ids, account_ids, now, hours, th.grace_minutes).await?);
    }
    let snapshot = measure_snapshot(conn, graph_ids, account_ids, now, th).await?;
    Ok(Measured { windows, snapshot })
}

/// Dead letters per window, plane-wide.
pub async fn measure_dead_jobs(
    conn: &mut PgConnection,
    now: DateTime<Utc>,
    windows_hours: &[i64],
) -> Result<Vec<(i64, Vec<KindCount>)>, sqlx::Error> {
    let mut out = Vec::new();
    for &hours in windows_hours {
        let rows = sqlx::query_as(DEAD_JOBS_SQL)
            .bind(now - Duration::hours(hours))
            .bind(now)
            .fetch_all(&mut *conn)
            .await?;
        out.push((hours, rows));
    }
    Ok(out)
}

/// Control-plane credit holds past the threshold for one account on one
/// plane. The caller scopes `control` to `account`
/// (`managed_credit_reservations` is account-RLS'd).
pub async fn measure_credit_holds(
    control: &mut PgConnection,
    account: Uuid,
    plane: &str,
    now: DateTime<Utc>,
    th: &Thresholds,
) -> Result<Option<StuckHolds>, sqlx::Error> {
    let row: StuckHolds = sqlx::query_as(CREDIT_HOLDS_SQL)
        .bind(account)
        .bind(now - Duration::minutes(th.hold_max_minutes))
        .bind(plane)
        .fetch_one(control)
        .await?;
    Ok((row.holds > 0).then_some(row))
}

// ── Rules ────────────────────────────────────────────────────────────────────

/// One alertable condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    /// Stable event name (`pipeline_*`) the pager routes on.
    pub event: String,
    pub tenant: String,
    /// `None` for plane-level stages.
    pub source_type: Option<String>,
    pub window_hours: i64,
    pub message: String,
    pub detail: serde_json::Value,
}

impl Finding {
    pub fn key(&self) -> String {
        format!("{}/{}/{}", self.event, self.tenant, self.source_type.as_deref().unwrap_or("*"))
    }
}

fn baseline_source<'a>(report: &'a StageReport, th: &Thresholds, source_type: &str) -> Option<&'a SourceStages> {
    report.window(th.baseline_hours).and_then(|w| w.source(source_type))
}

fn minutes_since(now: DateTime<Utc>, at: Option<DateTime<Utc>>) -> Option<i64> {
    at.map(|at| (now - at).num_minutes())
}

/// The alert rules. A stage that should flow alerts when it drops to zero
/// while its upstream is flowing, or when its output degrades past a
/// threshold. Pure: stored reports are re-evaluated at read time.
pub fn evaluate(report: &StageReport, th: &Thresholds) -> Vec<Finding> {
    let mut out = Vec::new();
    let tenant = report.tenant.clone();
    let now = report.measured_at;
    let push = |out: &mut Vec<Finding>, event: &str, source: Option<&str>, hours: i64, message: String, detail: serde_json::Value| {
        out.push(Finding {
            event: event.to_string(),
            tenant: tenant.clone(),
            source_type: source.map(str::to_string),
            window_hours: hours,
            message,
            detail,
        })
    };

    // Embedding: captures flowing, nothing embedded or deliberately skipped.
    if let Some(w) = (th.embed_stall_hours > 0).then(|| report.window(th.embed_stall_hours)).flatten() {
        for s in &w.sources {
            if s.atoms >= th.min_flow && s.embedded == 0 && s.embed_skipped == 0 {
                push(&mut out, "pipeline_embed_stalled", Some(&s.source_type), w.hours,
                    format!("{} {} atoms captured in {}h, none embedded", s.atoms, s.source_type, w.hours),
                    serde_json::json!({"atoms": s.atoms, "blocked": s.embed_blocked, "held": s.embed_held,
                        "failed": s.embed_failed, "pending": s.embed_pending(),
                        "oldest_unembedded_minutes": minutes_since(now, s.oldest_unembedded_at)}));
            }
        }
    }

    // Extraction: unheld waits accumulating, nothing done.
    if let Some(w) = (th.extraction_stall_hours > 0).then(|| report.window(th.extraction_stall_hours)).flatten() {
        for s in &w.sources {
            let expected = baseline_source(report, th, &s.source_type).is_some_and(|b| b.extraction_done > 0);
            if expected && s.extraction_done == 0 && s.extraction_waiting >= th.min_flow {
                push(&mut out, "pipeline_extraction_stalled", Some(&s.source_type), w.hours,
                    format!("{} {} versions waiting for extraction, none done in {}h", s.extraction_waiting, s.source_type, w.hours),
                    serde_json::json!({"waiting": s.extraction_waiting, "held": s.extraction_held,
                        "blocked": s.extraction_blocked, "failed": s.extraction_failed,
                        "oldest_waiting_minutes": minutes_since(now, s.oldest_waiting_at),
                        "standing_processing_enabled": report.switches.as_ref().map(|s| s.standing_processing_enabled)}));
            }
        }
    }

    // Linking: extraction done, but no chunk rows, or no reef links at all.
    if let Some(w) = (th.link_stall_hours > 0).then(|| report.window(th.link_stall_hours)).flatten() {
        for s in &w.sources {
            if s.done_without_chunks >= th.min_flow {
                push(&mut out, "pipeline_chunks_missing", Some(&s.source_type), w.hours,
                    format!("{} of {} extracted {} versions have no chunk rows", s.done_without_chunks, s.extraction_done, s.source_type),
                    serde_json::json!({"done": s.extraction_done, "without_chunks": s.done_without_chunks}));
            }
            let expected = baseline_source(report, th, &s.source_type).is_some_and(|b| b.done_linked > 0);
            if expected && s.extraction_done >= th.min_flow && s.done_linked == 0 {
                push(&mut out, "pipeline_linking_stalled", Some(&s.source_type), w.hours,
                    format!("{} {} versions extracted in {}h, none linked to a reef", s.extraction_done, s.source_type, w.hours),
                    serde_json::json!({"done": s.extraction_done, "linked": s.done_linked,
                        "node_atoms_bridge_enabled": report.switches.as_ref().map(|s| s.node_atoms_bridge_enabled)}));
            }
        }
    }

    // Reefs: duplicates of existing reefs, and ER not judging them.
    if let Some(w) = (th.reef_window_hours > 0).then(|| report.window(th.reef_window_hours)).flatten() {
        let total = w.totals();
        if total.reefs_minted >= th.dup_min_minted {
            let rate = total.reefs_minted_duplicate as f64 / total.reefs_minted as f64;
            if rate > th.dup_rate_max {
                push(&mut out, "pipeline_reef_duplicates", None, w.hours,
                    format!("{:.0}% of {} reefs minted in {}h repeat an existing reef's title", rate * 100.0, total.reefs_minted, w.hours),
                    serde_json::json!({"minted": total.reefs_minted, "duplicates": total.reefs_minted_duplicate,
                        "rate": rate, "max_rate": th.dup_rate_max,
                        "by_source": w.sources.iter().filter(|s| s.reefs_minted_duplicate > 0)
                            .map(|s| serde_json::json!({"source_type": s.source_type, "minted": s.reefs_minted, "duplicates": s.reefs_minted_duplicate}))
                            .collect::<Vec<_>>()}));
            }
        }
        if total.reefs_minted_duplicate >= th.min_flow && w.graph.er_judgements == 0 && w.graph.er_merges == 0 {
            push(&mut out, "pipeline_er_stalled", None, w.hours,
                format!("{} duplicate reefs minted in {}h, EntityResolution judged and merged nothing", total.reefs_minted_duplicate, w.hours),
                serde_json::json!({"duplicates": total.reefs_minted_duplicate,
                    "enrichment_processing_enabled": report.switches.as_ref().map(|s| s.enrichment_processing_enabled)}));
        }
    }

    // Relationship edges: extraction flowing, no edges, though there used to be.
    if let Some(w) = (th.edge_stall_hours > 0).then(|| report.window(th.edge_stall_hours)).flatten() {
        let done = w.totals().extraction_done;
        let expected = report.window(th.baseline_hours).is_some_and(|b| b.graph.relationship_edges > 0);
        if expected && done >= th.min_flow && w.graph.relationship_edges == 0 {
            push(&mut out, "pipeline_edges_stalled", None, w.hours,
                format!("{done} versions extracted in {}h, no relationship edges", w.hours),
                serde_json::json!({"extraction_done": done}));
        }
    }

    // Journal: captures flowing, no hourly rollups, though there used to be.
    if let Some(w) = (th.journal_stall_hours > 0).then(|| report.window(th.journal_stall_hours)).flatten() {
        let atoms = w.totals().atoms;
        let expected = report.window(th.baseline_hours).is_some_and(|b| b.graph.journal_hourly_rollups > 0);
        if expected && atoms >= th.min_flow && w.graph.journal_hourly_rollups == 0 {
            push(&mut out, "pipeline_journal_stalled", None, w.hours,
                format!("{atoms} atoms captured in {}h, no hourly journal rollups", w.hours),
                serde_json::json!({"atoms": atoms}));
        }
    }

    // Titles: no non-fallback title at all, for a type that used to get them.
    if let Some(w) = (th.title_window_hours > 0).then(|| report.window(th.title_window_hours)).flatten() {
        for s in &w.sources {
            let expected = baseline_source(report, th, &s.source_type).is_some_and(|b| b.real_titles > 0);
            if expected && s.atoms >= th.title_min_atoms && s.real_titles == 0 {
                push(&mut out, "pipeline_titles_stalled", Some(&s.source_type), w.hours,
                    format!("{} {} atoms in {}h, every one with a fallback title", s.atoms, s.source_type, w.hours),
                    serde_json::json!({"atoms": s.atoms, "fallback": s.fallback_titles}));
            }
        }
    }

    // Jev binding: mostly withheld, errored or timed out.
    if let Some(w) = (th.jev_window_hours > 0).then(|| report.window(th.jev_window_hours)).flatten() {
        let g = &w.graph;
        let timed_out = report.snapshot.jev_timed_out;
        let calls = g.jev_ok + g.jev_withheld + g.jev_errored + timed_out;
        if calls >= th.jev_min_calls {
            let success = g.jev_ok as f64 / calls as f64;
            if success < th.jev_min_success {
                push(&mut out, "pipeline_jev_degraded", None, w.hours,
                    format!("Jev binding answered {} of {calls} calls in {}h", g.jev_ok, w.hours),
                    serde_json::json!({"ok": g.jev_ok, "withheld": g.jev_withheld, "errored": g.jev_errored,
                        "timed_out_pending": timed_out, "success": success, "min_success": th.jev_min_success,
                        "jev_binding_enabled": report.switches.as_ref().map(|s| s.jev_binding_enabled)}));
            }
        }
    }

    // Dead letters: a burst of one job kind.
    if let Some(w) = (th.dead_jobs_hours > 0).then(|| report.window(th.dead_jobs_hours)).flatten() {
        for k in w.dead_jobs.iter().filter(|k| k.count > th.dead_jobs_max) {
            push(&mut out, "pipeline_jobs_dead", Some(&k.kind), w.hours,
                format!("{} {} jobs dead-lettered in {}h", k.count, k.kind, w.hours),
                serde_json::json!({"kind": k.kind, "dead": k.count, "max": th.dead_jobs_max}));
        }
    }

    // Managed spend at the daily cap, and reservations that never settled.
    for s in report.snapshot.spend.iter().filter(|s| s.used_fraction() >= th.spend_cap_fraction) {
        push(&mut out, "pipeline_spend_at_cap", Some(&s.purpose), 0,
            format!("managed {} spend is at {:.0}% of today's cap", s.purpose, s.used_fraction() * 100.0),
            serde_json::json!({"account_id": s.account_id, "limit_micro_usd": s.limit_micro_usd,
                "reserved_micro_usd": s.reserved_micro_usd, "charged_micro_usd": s.charged_micro_usd}));
    }
    for h in report.snapshot.stuck_holds.iter().filter(|h| h.holds > 0) {
        push(&mut out, "pipeline_hold_stuck", Some(&h.purpose), 0,
            format!("{} managed {} holds older than {} minutes", h.holds, h.purpose, th.hold_max_minutes),
            serde_json::json!({"holds": h.holds, "oldest_minutes": minutes_since(now, h.oldest_at)}));
    }
    out
}

// ── Operator table ───────────────────────────────────────────────────────────

fn age(now: DateTime<Utc>, at: Option<DateTime<Utc>>) -> String {
    match minutes_since(now, at) {
        None => "-".into(),
        Some(m) if m < 120 => format!("{m}m"),
        Some(m) if m < 48 * 60 => format!("{}h", m / 60),
        Some(m) => format!("{}d", m / (24 * 60)),
    }
}

fn pct(part: i64, whole: i64) -> String {
    if whole == 0 { "-".into() } else { format!("{}%", part * 100 / whole) }
}

/// The per-stage, per-source-type table for one window, then the plane
/// stages, the snapshot and the findings.
pub fn render_table(report: &StageReport, hours: i64, findings: &[Finding]) -> String {
    let mut t = String::new();
    let now = report.measured_at;
    let _ = writeln!(
        t,
        "tenant {}  measured {}  window {}h (cohort excludes the last {}m)  accounts {} (+{} processing off)",
        report.tenant, now.format("%Y-%m-%d %H:%MZ"), hours, report.grace_minutes,
        report.accounts_measured, report.accounts_processing_off
    );
    match &report.switches {
        Some(sw) => {
            let _ = writeln!(
                t,
                "switches standing={} enrichment={} managed_disabled={} node_atoms_bridge={} jev_binding={}",
                sw.standing_processing_enabled, sw.enrichment_processing_enabled, sw.managed_processing_disabled,
                sw.node_atoms_bridge_enabled, sw.jev_binding_enabled
            );
        }
        None => {
            let _ = writeln!(t, "switches: not measured (read from a database, not a serving process)");
        }
    }
    let Some(w) = report.window(hours) else {
        let _ = writeln!(t, "no {hours}h window in this report (have: {:?})", report.windows.iter().map(|w| w.hours).collect::<Vec<_>>());
        return t;
    };
    let _ = writeln!(
        t,
        "\n{:<22} {:>6} {:>6} {:>5} {:>5} {:>5} {:>5} {:>6} | {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} | {:>6} {:>6} | {:>6} {:>5} {:>5} {:>5} | {:>5}",
        "source_type", "atoms", "embed", "skip", "blk", "fail", "pend", "oldest",
        "vers", "done", "fail", "blk", "wait", "held", "oldest",
        "nochnk", "linked",
        "minted", "dup", "new", "exist", "fb%"
    );
    let mut rows: Vec<&SourceStages> = w.sources.iter().collect();
    let total = w.totals();
    rows.push(&total);
    for s in rows {
        let _ = writeln!(
            t,
            "{:<22} {:>6} {:>6} {:>5} {:>5} {:>5} {:>5} {:>6} | {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>6} | {:>6} {:>6} | {:>6} {:>5} {:>5} {:>5} | {:>5}",
            s.source_type, s.atoms, s.embedded, s.embed_skipped, s.embed_blocked, s.embed_failed, s.embed_pending(),
            age(now, s.oldest_unembedded_at),
            s.versions, s.extraction_done, s.extraction_failed, s.extraction_blocked, s.extraction_waiting, s.extraction_held,
            age(now, s.oldest_waiting_at),
            s.done_without_chunks, s.done_linked,
            s.reefs_minted, s.reefs_minted_duplicate, s.links_to_new, s.links_to_existing,
            pct(s.fallback_titles, s.atoms)
        );
    }
    let g = &w.graph;
    let _ = writeln!(
        t,
        "\nplane: edges {}  ER judgements {} (+{} ?{})  merges {}  journal hourly {}  jev ok {} withheld {} errored {} timed-out-now {}",
        g.relationship_edges, g.er_judgements, g.er_positive, g.er_unsure, g.er_merges, g.journal_hourly_rollups,
        g.jev_ok, g.jev_withheld, g.jev_errored, report.snapshot.jev_timed_out
    );
    if !w.dead_jobs.is_empty() {
        let dead: Vec<String> = w.dead_jobs.iter().map(|k| format!("{} {}", k.kind, k.count)).collect();
        let _ = writeln!(t, "dead jobs: {}", dead.join(", "));
    }
    let runs: Vec<String> = report
        .snapshot
        .standing_runs
        .iter()
        .map(|r| format!("{} {}/{} x{}", r.extraction_version, r.status, r.pause_state.as_deref().unwrap_or("-"), r.runs))
        .collect();
    let _ = writeln!(t, "standing runs: {}", if runs.is_empty() { "none".into() } else { runs.join(", ") });
    for s in &report.snapshot.spend {
        let _ = writeln!(
            t,
            "spend today {} {}: ${:.4} charged + ${:.4} reserved of ${:.2} ({:.0}%)",
            s.account_id, s.purpose, s.charged_micro_usd as f64 / 1e6, s.reserved_micro_usd as f64 / 1e6,
            s.limit_micro_usd as f64 / 1e6, s.used_fraction() * 100.0
        );
    }
    for h in &report.snapshot.stuck_holds {
        let _ = writeln!(t, "stuck holds {}: {} (oldest {})", h.purpose, h.holds, age(now, h.oldest_at));
    }
    let _ = writeln!(t, "\nfindings: {}", findings.len());
    for f in findings {
        let _ = writeln!(t, "  [{}] {}", f.event, f.message);
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(windows: Vec<WindowStages>) -> StageReport {
        StageReport {
            tenant: "org_t".into(),
            measured_at: Utc::now(),
            grace_minutes: 15,
            accounts_measured: 1,
            accounts_processing_off: 0,
            switches: None,
            windows,
            snapshot: Snapshot::default(),
        }
    }

    fn src(t: &str) -> SourceStages {
        SourceStages { source_type: t.into(), ..Default::default() }
    }

    fn window(hours: i64, sources: Vec<SourceStages>) -> WindowStages {
        WindowStages { hours, sources, ..Default::default() }
    }

    fn events(f: &[Finding]) -> Vec<&str> {
        f.iter().map(|f| f.event.as_str()).collect()
    }

    #[test]
    fn windows_are_distinct_and_skip_disabled_rules() {
        let mut th = Thresholds::default();
        assert_eq!(th.windows(), vec![2, 6, 24, 168]);
        th.embed_stall_hours = 0;
        th.dead_jobs_hours = 0;
        assert_eq!(th.windows(), vec![6, 24, 168]);
    }

    #[test]
    fn embed_stall_needs_flow_and_ignores_skipped() {
        let th = Thresholds::default();
        let stalled = SourceStages { atoms: 5, embed_blocked: 5, ..src("slack") };
        assert_eq!(events(&evaluate(&report(vec![window(2, vec![stalled.clone()])]), &th)), vec!["pipeline_embed_stalled"]);
        let skipped = SourceStages { embed_skipped: 5, embed_blocked: 0, ..stalled.clone() };
        assert!(evaluate(&report(vec![window(2, vec![skipped])]), &th).is_empty());
        let trickle = SourceStages { atoms: 2, ..stalled };
        assert!(evaluate(&report(vec![window(2, vec![trickle])]), &th).is_empty());
    }

    #[test]
    fn extraction_stall_needs_a_baseline_and_ignores_holds() {
        let th = Thresholds::default();
        let w6 = window(6, vec![SourceStages { versions: 4, extraction_waiting: 4, ..src("github_code") }]);
        let base = |done| window(168, vec![SourceStages { extraction_done: done, ..src("github_code") }]);
        assert_eq!(events(&evaluate(&report(vec![w6.clone(), base(9)]), &th)), vec!["pipeline_extraction_stalled"]);
        assert!(evaluate(&report(vec![w6.clone(), base(0)]), &th).is_empty(), "never extracted: not expected");
        let held = window(6, vec![SourceStages { versions: 4, extraction_held: 4, ..src("github_code") }]);
        assert!(evaluate(&report(vec![held, base(9)]), &th).is_empty());
    }

    #[test]
    fn duplicate_rate_and_spend_and_holds() {
        let th = Thresholds::default();
        let mut r = report(vec![window(24, vec![SourceStages { reefs_minted: 20, reefs_minted_duplicate: 10, ..src("slack") }])]);
        r.windows[0].graph.er_merges = 1;
        assert_eq!(events(&evaluate(&r, &th)), vec!["pipeline_reef_duplicates"]);
        r.windows[0].graph.er_merges = 0;
        assert_eq!(events(&evaluate(&r, &th)), vec!["pipeline_reef_duplicates", "pipeline_er_stalled"]);
        let mut r = report(vec![]);
        r.snapshot.spend.push(PurposeSpend { account_id: Uuid::nil(), purpose: "source_extraction".into(), limit_micro_usd: 100, reserved_micro_usd: 10, charged_micro_usd: 90 });
        r.snapshot.stuck_holds.push(StuckHolds { purpose: "cloud_embedding".into(), holds: 2, oldest_at: None });
        assert_eq!(events(&evaluate(&r, &th)), vec!["pipeline_spend_at_cap", "pipeline_hold_stuck"]);
    }

    #[test]
    fn jev_counts_pending_timeouts() {
        let th = Thresholds::default();
        let mut r = report(vec![window(6, vec![])]);
        r.windows[0].graph.jev_ok = 2;
        r.snapshot.jev_timed_out = 4;
        assert_eq!(events(&evaluate(&r, &th)), vec!["pipeline_jev_degraded"]);
        r.windows[0].graph.jev_ok = 20;
        assert!(evaluate(&r, &th).is_empty());
    }

    #[test]
    fn table_renders_every_source_and_the_total() {
        let r = report(vec![window(24, vec![SourceStages { atoms: 3, ..src("slack") }, src("linear_issue")])]);
        let table = render_table(&r, 24, &[]);
        assert!(table.contains("slack") && table.contains("linear_issue") && table.contains("\n*"));
        assert!(render_table(&r, 6, &[]).contains("no 6h window"));
    }
}
