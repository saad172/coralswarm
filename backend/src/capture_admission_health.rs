//! Capture-to-Atom health (COR-71): how long captures wait to become Atoms,
//! and which ones are stuck.
//!
//! SLO (COR-20 decision 8): a capture becomes an Atom at p95 within 10
//! minutes, p99 within 30, and never later than 60. Any capture still waiting
//! after [`ALERT_AFTER`] is an alert, unless the wait is a decision someone
//! made: the account switched processing off, an operator paused the run, the
//! source was removed, or the capture is synthetic. Those are reported as
//! `held` and never trigger the stall alert. Unknown durable clock history
//! remains a separate red diagnostic, including after completion; a receipt
//! from before the clock existed is `legacy`, and a carried re-capture is
//! `carried`, not unknown (see [`ClockClass`]). Eligible
//! percentiles use protected never-held receipt clocks: `never_held`, or
//! `standing_admitted` for a capture standing adopted fresh. Disposition comes
//! from live facts, not from the
//! receipt's reason text, because a stale reason such as
//! `cloud_processing_disabled` outlives the setting that caused it.
//!
//! `workers::capture_admission_health` measures each plane and stores a
//! [`PlaneReport`]; `routes::admin::admin_chain_health` turns the stored
//! reports into a [`Verdict`].
//!
//! Two tiers (2026-10-21). A **page** is a capture nothing is moving: an
//! unheld capture past [`ALERT_AFTER`] in a graph that completed no
//! extraction window for [`STALL_AFTER`], an unheld capture outside the queue
//! (blocked without a decision, unreceipted) past [`STUCK_AFTER`], a queued
//! capture whose receipt has not changed state for [`STUCK_QUEUED_AFTER`], or a plane
//! nobody measured. An **SLO breach** is a backlog that is draining too
//! slowly: live-capture lag over the SLO, live queued captures past an hour,
//! and unknown clock history. A draining backlog is not a stall: before this,
//! a re-extraction backlog of 2,300 queued captures read as 2,300 stalled
//! captures, and the hourly check failed for a day while the queue moved.
//!
//! Only live captures count toward the SLO ([`LIVE_CAPTURE`]): a re-capture of
//! old content is operator backfill, not a capture someone is waiting on.

use std::collections::HashSet;

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use uuid::Uuid;

pub const ALERT_AFTER: Duration = Duration::minutes(15);
/// A plane whose report is older than this has stopped being measured. The
/// harness polls a dormant org tenant every 20 minutes.
pub const STALE_AFTER: Duration = Duration::minutes(45);
pub const LAG_WINDOW: Duration = Duration::hours(24);
pub const SLO_P95_SECS: f64 = 600.0;
pub const SLO_P99_SECS: f64 = 1800.0;
pub const SLO_MAX_SECS: f64 = 3600.0;
pub const CONSUMER_TENANT: &str = "__consumer__";
/// A graph that completed an extraction window this recently is draining:
/// its queued captures are backlog, not a stall.
pub const STALL_AFTER: Duration = Duration::minutes(30);
/// An unheld capture outside the queue this old is stuck even in a graph that
/// is draining: nothing is retrying it.
pub const STUCK_AFTER: Duration = Duration::hours(6);
/// A capture queued this long without its receipt changing state is stranded,
/// even in a graph that is draining: the backlog lane serves the oldest
/// queued work first, so a draining queue reaches it well before this.
pub const STUCK_QUEUED_AFTER: Duration = Duration::hours(48);

/// A live capture: its content is from about when it was captured, it has no
/// source time, or its content differs from the item's previous version (an
/// edit, such as a Slack message edited days after it was posted, keeps the
/// message's original time). A re-capture of history is not: `occurred_at`
/// more than a day before `captured_at`, and either the first capture of old
/// content or the same content as the previous version (2026-10-20: 2,113
/// Slack and 182 Linear re-captures, all byte-identical or first captures of
/// week-old content). `v` is `source_versions`. Shared with the standing
/// fresh lane (`workers::source_atoms`), so the lane serves exactly the
/// captures the SLO measures.
macro_rules! live_capture {
    () => {
        "(v.occurred_at IS NULL OR v.occurred_at>=v.captured_at-interval '24 hours' \
          OR COALESCE((SELECT p.content_sha256 IS DISTINCT FROM v.content_sha256 FROM public.source_versions p \
            WHERE p.graph_id=v.graph_id AND p.source_item_id=v.source_item_id AND (p.captured_at,p.version_id)<(v.captured_at,v.version_id) \
            ORDER BY p.captured_at DESC,p.version_id DESC LIMIT 1),false))"
    };
}
pub const LIVE_CAPTURE: &str = live_capture!();

/// Captures before this are older history, owned by explicit cohort runs
/// (COR-20 decision 6), and are never expected to drain on their own.
pub fn adoption_floor() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 13, 0, 0, 0).unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Stalled,
    Held,
}

/// Captures in one graph that share a receipt state and reason and are not
/// Atoms yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitingCaptures {
    pub graph_id: Uuid,
    /// The admission receipt state, or `unreceipted` when capture wrote none.
    pub state: String,
    pub reason: Option<String>,
    pub disposition: Disposition,
    /// Why a held group is held; `None` for stalled groups.
    pub hold: Option<String>,
    pub captures: i64,
    pub over_alert_age: i64,
    pub oldest_captured_at: DateTime<Utc>,
    #[serde(default)]
    pub never_held: bool,
    #[serde(default)]
    pub over_lag_age: i64,
    /// Every capture in the group is live ([`LIVE_CAPTURE`]).
    #[serde(default = "default_live")]
    pub live: bool,
    /// The oldest receipt state change in the group (`updated_at`): for a
    /// queued group, how long its oldest capture has sat queued. `None` in a
    /// report from an older binary.
    #[serde(default)]
    pub oldest_queued_at: Option<DateTime<Utc>>,
}

fn default_live() -> bool {
    true
}

/// When a graph last completed an extraction window, within the trailing
/// [`LAG_WINDOW`]. A graph with no row completed none.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct GraphProgress {
    pub graph_id: Uuid,
    pub last_window_at: DateTime<Utc>,
    pub windows: i64,
}

/// Receipt-clock lag of verified never-held captures completed in the
/// trailing [`LAG_WINDOW`]. Held and unknown candidate counts remain separate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphLag {
    pub graph_id: Uuid,
    pub processed: i64,
    pub p50_secs: f64,
    pub p95_secs: f64,
    pub p99_secs: f64,
    pub max_secs: f64,
    pub slo_met: bool,
}

/// What a capture's receipt clock can establish. Only `Unknown` is red: a
/// receipt that should carry a clock and does not, or one that contradicts
/// its source.
///
/// `Legacy` is a receipt written before the clock columns existed (tenant
/// 0151 / control 0319 added `hold_basis` with the `legacy_unknown` default,
/// then switched the default; the observation trigger stamps every later
/// insert `unverified_transition` and keeps `legacy_unknown` sticky). No
/// runtime path can write it, so a count of them is a constant of the
/// migration, not a signal, and it never clears: 2026-10-25 production had
/// 5,138 org-tenant and 26 consumer receipts observed 2026-10-13 07:10 to
/// 2026-10-16 03:20, holding the capture SLO breached for a week with every
/// lag sample inside the SLO. Its identity and source-state checks still
/// apply; only its missing clock is not a defect.
///
/// `Carried` is a byte-identical metadata-only re-capture recorded `done`
/// at capture (`source_evidence::carries_extraction`). It waited for
/// nothing, so it is neither an eligible lag sample nor unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all="snake_case")]
#[sqlx(type_name="text",rename_all="snake_case")]
pub enum ClockClass { NeverHeld, Held, Backlog, Unknown, Legacy, Carried }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct ClockCoverage {
    pub graph_id: Uuid,
    pub account_id: Uuid,
    pub class: ClockClass,
    pub hold_basis: Option<String>,
    pub captures: i64,
    pub processed: i64,
    pub waiting: i64,
    #[sqlx(default)]
    pub processing_off: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaneReport {
    pub tenant: String,
    pub measured_at: DateTime<Utc>,
    pub lag: Vec<GraphLag>,
    pub waiting: Vec<WaitingCaptures>,
    #[serde(default)]
    pub clock_coverage: Option<Vec<ClockCoverage>>,
    /// `None` in a report from a binary before progress was measured: every
    /// unheld capture past [`ALERT_AFTER`] then pages, as before.
    #[serde(default)]
    pub progress: Option<Vec<GraphProgress>>,
}

impl PlaneReport {
    /// Unheld captures past [`ALERT_AFTER`] that nothing is moving.
    pub fn stalled(&self) -> impl Iterator<Item = &WaitingCaptures> {
        self.waiting
            .iter()
            .filter(|w| w.disposition == Disposition::Stalled && w.over_alert_age > 0)
            .filter(|w| !self.draining(w) || self.stuck(w))
    }

    /// Nothing will move this group even though its graph is draining: outside
    /// the queue past [`STUCK_AFTER`], or queued without a state change past
    /// [`STUCK_QUEUED_AFTER`].
    fn stuck(&self, w: &WaitingCaptures) -> bool {
        if w.state == "queued" {
            w.oldest_queued_at.is_some_and(|at| self.measured_at - at > STUCK_QUEUED_AFTER)
        } else {
            self.measured_at - w.oldest_captured_at > STUCK_AFTER
        }
    }

    /// The group's graph completed a window within [`STALL_AFTER`].
    fn draining(&self, w: &WaitingCaptures) -> bool {
        self.progress.as_ref().is_some_and(|progress| {
            progress.iter().any(|p| p.graph_id == w.graph_id && self.measured_at - p.last_window_at <= STALL_AFTER)
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct WaitingRow {
    pub graph_id: Uuid,
    pub account_id: Uuid,
    pub state: String,
    pub reason: Option<String>,
    pub synthetic: bool,
    pub operator_paused: bool,
    pub source_permitted: bool,
    pub captures: i64,
    pub over_alert_age: i64,
    pub oldest_captured_at: DateTime<Utc>,
    pub never_held: bool,
    pub over_lag_age: i64,
    pub live: bool,
    pub oldest_queued_at: Option<DateTime<Utc>>,
}

/// A capture waits until its version is `done` or `failed` and its receipt is
/// terminal. `account` scopes the consumer plane; an org tenant passes `None`.
const WAITING_SQL: &str = concat!("\
SELECT v.graph_id, g.account_id, \
  COALESCE(r.state,'unreceipted') AS state, COALESCE(r.reason,v.extraction_error) AS reason, \
  (COALESCE(v.origin->>'transport'='synthetic',false) OR COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false)) AS synthetic, \
  EXISTS(SELECT 1 FROM corpus_enrichment_units u JOIN corpus_enrichment_runs run ON run.run_id=u.run_id AND run.graph_id=u.graph_id \
    WHERE u.graph_id=v.graph_id AND COALESCE(u.promoted_version_id,u.holding_id)=v.version_id AND u.execution_owner \
      AND run.status='paused' AND run.pause_reason='operator_paused') AS operator_paused, \
  EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted') AS source_permitted, \
  (r.hold_basis IN ('never_held','standing_admitted') AND NOT r.ever_held AND r.graph_id=v.graph_id AND r.account_id=g.account_id AND r.source_item_id=v.source_item_id AND r.extraction_version=v.extraction_version) IS TRUE AS never_held, \
  r.observed_at, v.captured_at, COALESCE(r.updated_at,v.captured_at) AS queued_at, ", live_capture!(), " AS live \
FROM source_versions v JOIN graphs g ON g.id=v.graph_id \
LEFT JOIN corpus_capture_admissions r ON r.source_version_id=v.version_id \
WHERE v.captured_at >= $1 AND ($2::uuid IS NULL OR g.account_id=$2) \
  AND v.extraction_status NOT IN ('done','failed') \
  AND COALESCE(r.state,'unreceipted') NOT IN ('processed','failed','unavailable','excluded')");

pub async fn waiting(
    conn: &mut PgConnection,
    account: Option<Uuid>,
    now: DateTime<Utc>,
) -> Result<Vec<WaitingRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT graph_id, account_id, state, reason, synthetic, operator_paused, source_permitted, never_held, live, \
           count(*) AS captures, count(*) FILTER (WHERE captured_at < $3) AS over_alert_age, \
           count(*) FILTER (WHERE never_held AND observed_at < $4) AS over_lag_age, min(captured_at) AS oldest_captured_at, min(queued_at) AS oldest_queued_at \
         FROM ({WAITING_SQL}) w GROUP BY 1,2,3,4,5,6,7,8,9 ORDER BY 1,3,4,9"
    ))
    .bind(adoption_floor())
    .bind(account)
    .bind(now - ALERT_AFTER)
    .bind(now - Duration::seconds(SLO_MAX_SECS as i64))
    .fetch_all(conn)
    .await
}

/// Every capture since [`adoption_floor`], by [`ClockClass`]. A missing
/// receipt, a receipt that does not match its source, or a source/receipt
/// state contradiction is `unknown` before any basis is read, so a legacy or
/// carried label never hides one.
pub async fn clock_coverage(conn: &mut PgConnection, account: Option<Uuid>) -> Result<Vec<ClockCoverage>, sqlx::Error> {
    sqlx::query_as("SELECT graph_id,account_id,class,hold_basis,count(*) AS captures,count(*) FILTER (WHERE processed) AS processed,count(*) FILTER (WHERE waiting) AS waiting,false AS processing_off FROM (SELECT v.graph_id,g.account_id,r.hold_basis,CASE WHEN r.source_version_id IS NULL OR r.graph_id<>v.graph_id OR r.account_id<>g.account_id OR r.source_item_id<>v.source_item_id OR r.extraction_version<>v.extraction_version THEN 'unknown' WHEN v.extraction_status='done' AND r.state<>'processed' THEN 'unknown' WHEN r.state='processed' AND v.extraction_status<>'done' THEN 'unknown' WHEN r.hold_basis='legacy_unknown' THEN 'legacy' WHEN r.state='processed' AND (r.processed_at IS NULL OR r.processed_at<r.observed_at) THEN 'unknown' WHEN r.hold_basis='unverified_transition' THEN 'unknown' WHEN r.hold_basis='backlog_policy_excluded' THEN 'backlog' WHEN r.hold_basis='carried_forward' AND NOT r.ever_held THEN 'carried' WHEN r.hold_basis IN ('never_held','standing_admitted') AND NOT r.ever_held THEN 'never_held' WHEN r.ever_held AND r.hold_basis NOT IN ('never_held','standing_admitted','carried_forward') THEN 'held' ELSE 'unknown' END AS class,(r.state='processed' AND r.processed_at IS NOT NULL AND v.extraction_status='done') IS TRUE AS processed,v.extraction_status NOT IN ('done','failed') AS waiting FROM source_versions v JOIN graphs g ON g.id=v.graph_id LEFT JOIN corpus_capture_admissions r ON r.source_version_id=v.version_id WHERE v.captured_at >= $1 AND ($2::uuid IS NULL OR g.account_id=$2)) candidate GROUP BY 1,2,3,4 ORDER BY 1,3,4")
        .bind(adoption_floor()).bind(account).fetch_all(conn).await
}

/// The last extraction window each graph completed in the trailing
/// [`LAG_WINDOW`]: a graph with a recent one is draining its queue.
pub async fn progress(conn: &mut PgConnection, account: Option<Uuid>, now: DateTime<Utc>) -> Result<Vec<GraphProgress>, sqlx::Error> {
    sqlx::query_as("SELECT w.graph_id,max(w.completed_at) AS last_window_at,count(*) AS windows FROM semantic_atom_windows w JOIN graphs g ON g.id=w.graph_id WHERE w.completed_at>=$1 AND w.completed_at<=$3 AND ($2::uuid IS NULL OR g.account_id=$2) GROUP BY w.graph_id ORDER BY w.graph_id")
        .bind(now - LAG_WINDOW).bind(account).bind(now).fetch_all(conn).await
}

/// Only durable never-held receipt clocks of live captures contribute to
/// eligible lag.
pub async fn lag(
    conn: &mut PgConnection,
    account: Option<Uuid>,
    now: DateTime<Utc>,
) -> Result<Vec<GraphLag>, sqlx::Error> {
    let rows: Vec<(Uuid, i64, f64, f64, f64, f64)> = sqlx::query_as(&format!(
        "SELECT graph_id,count(*),percentile_cont(0.5) WITHIN GROUP (ORDER BY lag),percentile_cont(0.95) WITHIN GROUP (ORDER BY lag),percentile_cont(0.99) WITHIN GROUP (ORDER BY lag),max(lag) FROM (SELECT v.graph_id,EXTRACT(EPOCH FROM r.processed_at-r.observed_at)::float8 AS lag FROM source_versions v JOIN graphs g ON g.id=v.graph_id JOIN corpus_capture_admissions r ON r.source_version_id=v.version_id AND r.graph_id=v.graph_id AND r.account_id=g.account_id AND r.source_item_id=v.source_item_id AND r.extraction_version=v.extraction_version WHERE v.extraction_status='done' AND r.state='processed' AND r.hold_basis IN ('never_held','standing_admitted') AND NOT r.ever_held AND r.processed_at>=r.observed_at AND r.processed_at>=$1 AND r.processed_at<=$3 AND v.captured_at>=$4 AND {live} AND NOT COALESCE(v.origin->>'transport'='synthetic',false) AND NOT COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false) AND ($2::uuid IS NULL OR g.account_id=$2)) eligible GROUP BY graph_id ORDER BY graph_id", live = LIVE_CAPTURE,
    ))
    .bind(now-LAG_WINDOW).bind(account).bind(now).bind(adoption_floor())
    .fetch_all(conn).await?;
    Ok(rows
        .into_iter()
        .map(|(graph_id, processed, p50, p95, p99, max)| GraphLag {
            graph_id,
            processed,
            p50_secs: p50,
            p95_secs: p95,
            p99_secs: p99,
            max_secs: max,
            slo_met: p95 <= SLO_P95_SECS && p99 <= SLO_P99_SECS && max <= SLO_MAX_SECS,
        })
        .collect())
}

/// Accounts that switched processing off via the `managed_processing`
/// preference set to `off` (COR-20 decision 1).
pub async fn accounts_processing_off(
    control: &mut PgConnection,
    accounts: &[Uuid],
) -> Result<HashSet<Uuid>, sqlx::Error> {
    let rows: Vec<(Uuid, Option<String>)> = sqlx::query_as(
        "SELECT account_id, prefs->>'managed_processing' \
         FROM account_prefs WHERE account_id = ANY($1)",
    )
    .bind(accounts)
    .fetch_all(control)
    .await?;
    Ok(rows
        .into_iter()
        .filter(|(_, managed)| managed.as_deref() == Some("off"))
        .map(|(account, _)| account)
        .collect())
}

fn hold(row: &WaitingRow, processing_off: &HashSet<Uuid>) -> Option<&'static str> {
    if processing_off.contains(&row.account_id) {
        Some("account_processing_off")
    } else if row.operator_paused {
        Some("operator_paused")
    } else if !row.source_permitted {
        Some("source_removed_or_access_lost")
    } else if row.synthetic {
        Some("synthetic_requires_explicit_run")
    } else {
        None
    }
}

pub fn classify(rows: Vec<WaitingRow>, processing_off: &HashSet<Uuid>) -> Vec<WaitingCaptures> {
    rows.into_iter()
        .map(|row| {
            let hold = hold(&row, processing_off);
            WaitingCaptures {
                graph_id: row.graph_id,
                state: row.state,
                reason: row.reason,
                disposition: if hold.is_some() { Disposition::Held } else { Disposition::Stalled },
                hold: hold.map(str::to_owned),
                captures: row.captures,
                over_alert_age: row.over_alert_age,
                oldest_captured_at: row.oldest_captured_at,
                never_held: row.never_held,
                over_lag_age: row.over_lag_age,
                live: row.live,
                oldest_queued_at: row.oldest_queued_at,
            }
        })
        .collect()
}

#[derive(Debug, Default, PartialEq)]
pub struct Verdict {
    pub stalled: Vec<(String, WaitingCaptures)>,
    /// Expected planes whose latest report is older than [`STALE_AFTER`].
    pub stale: Vec<String>,
    /// Expected planes with no report at all.
    pub missing: Vec<String>,
    pub unknown_clock: Vec<(String, ClockCoverage)>,
    pub unmeasured_clock: Vec<String>,
    pub lag_violations: Vec<(String, GraphLag)>,
    pub queued_lag_violations: Vec<(String, WaitingCaptures)>,
}

impl Verdict {
    pub fn violated(&self) -> bool {
        self.page() || self.slo_breached()
    }

    /// Nothing is moving a capture, or a plane is not measured: page on
    /// every check.
    pub fn page(&self) -> bool {
        !self.stalled.is_empty() || !self.stale.is_empty() || !self.missing.is_empty()
    }

    /// Captures are moving, but slower than the SLO, or their clock history
    /// cannot establish it. Worth one notification per change, not an hourly
    /// page (`.github/workflows/chain-health-check.yml`).
    pub fn slo_breached(&self) -> bool {
        !self.unknown_clock.is_empty() || !self.unmeasured_clock.is_empty()
            || !self.lag_violations.is_empty() || !self.queued_lag_violations.is_empty()
    }
}

/// Judges the stored reports of the planes that should be measured. A plane
/// nobody measured is a violation: a silent worker must not read as healthy.
pub fn verdict(reports: &[PlaneReport], expected: &[String], now: DateTime<Utc>) -> Verdict {
    let mut out = Verdict::default();
    for tenant in expected {
        match reports.iter().find(|r| &r.tenant == tenant) {
            None => out.missing.push(tenant.clone()),
            Some(r) if now - r.measured_at > STALE_AFTER => out.stale.push(tenant.clone()),
            Some(r) => {
                out.stalled.extend(r.stalled().map(|w| (tenant.clone(),w.clone())));
                match &r.clock_coverage {
                    None=>out.unmeasured_clock.push(tenant.clone()),
                    Some(coverage)=>out.unknown_clock.extend(coverage.iter().filter(|row|row.class==ClockClass::Unknown && row.captures>0).map(|row|(tenant.clone(),row.clone()))),
                }
                out.lag_violations.extend(r.lag.iter().filter(|row|!row.slo_met).map(|row|(tenant.clone(),row.clone())));
                out.queued_lag_violations.extend(r.waiting.iter().filter(|row|row.disposition==Disposition::Stalled && row.state=="queued" && row.never_held && row.live && row.over_lag_age>0).map(|row|(tenant.clone(),row.clone())));
            },
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(state: &str, reason: Option<&str>) -> WaitingRow {
        WaitingRow {
            graph_id: Uuid::nil(),
            account_id: Uuid::nil(),
            state: state.into(),
            reason: reason.map(str::to_owned),
            synthetic: false,
            operator_paused: false,
            source_permitted: true,
            captures: 3,
            over_alert_age: 2,
            oldest_captured_at: adoption_floor(),
            never_held: false, over_lag_age: 0, live: true, oldest_queued_at: None,
        }
    }

    fn report(tenant: &str, measured_at: DateTime<Utc>, waiting: Vec<WaitingCaptures>) -> PlaneReport {
        PlaneReport { tenant: tenant.into(), measured_at, lag: vec![], waiting, clock_coverage:Some(vec![]), progress: Some(vec![]) }
    }

    #[test]
    fn a_blocked_capture_without_a_decision_behind_it_stalls() {
        let out = classify(vec![row("blocked", Some("managed_admission_required"))], &HashSet::new());
        assert_eq!(out[0].disposition, Disposition::Stalled);
        assert_eq!(out[0].hold, None);
    }

    #[test]
    fn a_stale_disabled_reason_stalls_once_the_account_turned_processing_back_on() {
        let out = classify(vec![row("blocked", Some("cloud_processing_disabled"))], &HashSet::new());
        assert_eq!(out[0].disposition, Disposition::Stalled);
    }

    #[test]
    fn budget_and_credit_holds_stall() {
        for reason in ["daily_budget_exhausted", "credits_exhausted", "execution_budget_exhausted"] {
            let out = classify(vec![row("blocked", Some(reason))], &HashSet::new());
            assert_eq!(out[0].disposition, Disposition::Stalled, "{reason}");
        }
    }

    #[test]
    fn decisions_hold_instead_of_stalling() {
        let off: HashSet<Uuid> = [Uuid::nil()].into();
        let held = classify(vec![row("blocked", Some("managed_admission_required"))], &off);
        assert_eq!(held[0].hold.as_deref(), Some("account_processing_off"));

        let mut paused = row("blocked", Some("managed_run_paused"));
        paused.operator_paused = true;
        let mut removed = row("blocked", Some("managed_admission_required"));
        removed.source_permitted = false;
        let mut synthetic = row("blocked", Some("synthetic_requires_explicit_run"));
        synthetic.synthetic = true;
        let holds: Vec<_> = classify(vec![paused, removed, synthetic], &HashSet::new())
            .into_iter()
            .map(|w| (w.disposition, w.hold))
            .collect();
        assert_eq!(
            holds,
            [
                (Disposition::Held, Some("operator_paused".into())),
                (Disposition::Held, Some("source_removed_or_access_lost".into())),
                (Disposition::Held, Some("synthetic_requires_explicit_run".into())),
            ]
        );
    }

    #[test]
    fn verdict_alerts_only_on_stalled_captures_past_the_alert_age() {
        let now = Utc::now();
        let mut young = classify(vec![row("queued", None)], &HashSet::new());
        young[0].over_alert_age = 0;
        let held = classify(vec![row("blocked", None)], &[Uuid::nil()].into());
        let stuck = classify(vec![row("blocked", Some("managed_admission_required"))], &HashSet::new());
        let reports = [
            report("a", now, young),
            report("b", now, held),
            report("c", now, stuck.clone()),
        ];
        let expected = ["a".into(), "b".into(), "c".into()];
        let v = verdict(&reports, &expected, now);
        assert_eq!(v.stalled, vec![("c".to_string(), stuck[0].clone())]);
        assert!(v.violated());
    }

    #[test]
    fn a_draining_graph_pages_only_for_captures_nothing_retries() {
        let now = Utc::now();
        let group = |state: &str, age: Duration| {
            let mut w = classify(vec![row(state, None)], &HashSet::new()).remove(0);
            w.oldest_captured_at = now - age;
            w
        };
        let waiting = vec![
            group("queued", Duration::hours(20)),
            group("blocked", Duration::hours(1)),
            group("blocked", STUCK_AFTER + Duration::minutes(1)),
        ];
        let mut draining = report("t", now, waiting.clone());
        draining.progress = Some(vec![GraphProgress { graph_id: Uuid::nil(), last_window_at: now - STALL_AFTER, windows: 3 }]);
        let stalled: Vec<_> = draining.stalled().cloned().collect();
        assert_eq!(stalled, vec![waiting[2].clone()], "a moving queue pages only for a capture stuck outside it");

        let mut idle = draining.clone();
        idle.progress.as_mut().unwrap()[0].last_window_at = now - STALL_AFTER - Duration::seconds(1);
        assert_eq!(idle.stalled().count(), 3, "a graph that completed no window for STALL_AFTER is stalled");
        let mut unmeasured = draining.clone();
        unmeasured.progress = None;
        assert_eq!(unmeasured.stalled().count(), 3, "a report without progress pages as before");

        let mut slow = draining.clone();
        slow.waiting.truncate(2);
        slow.lag = vec![GraphLag { graph_id: Uuid::nil(), processed: 4, p50_secs: 60.0, p95_secs: 7200.0, p99_secs: 7200.0, max_secs: 7200.0, slo_met: false }];
        let v = verdict(&[slow], &["t".into()], now);
        assert!(!v.page() && v.slo_breached() && v.violated(), "{v:?}");
    }

    #[test]
    fn a_queued_capture_nothing_runs_for_48_hours_pages_in_a_draining_graph() {
        let now = Utc::now();
        let queued = |since: Duration| {
            let mut w = classify(vec![row("queued", None)], &HashSet::new()).remove(0);
            w.oldest_captured_at = now - Duration::days(5);
            w.oldest_queued_at = Some(now - since);
            w
        };
        let waiting = vec![queued(Duration::hours(47)), queued(STUCK_QUEUED_AFTER + Duration::minutes(1))];
        let mut draining = report("t", now, waiting.clone());
        draining.progress = Some(vec![GraphProgress { graph_id: Uuid::nil(), last_window_at: now, windows: 9 }]);
        let stalled: Vec<_> = draining.stalled().cloned().collect();
        assert_eq!(stalled, vec![waiting[1].clone()], "queued 48 h without a state change is stranded; an old capture re-queued recently is backlog");
    }

    #[test]
    fn verdict_flags_missing_and_stale_planes() {
        let now = Utc::now();
        let reports = [
            report("fresh", now - STALE_AFTER, vec![]),
            report("old", now - STALE_AFTER - Duration::seconds(1), vec![]),
            report("unexpected", now - Duration::days(9), vec![]),
        ];
        let expected = ["fresh".into(), "old".into(), "gone".into()];
        let v = verdict(&reports, &expected, now);
        assert_eq!(v.stale, vec!["old".to_string()]);
        assert_eq!(v.missing, vec!["gone".to_string()]);
        assert!(v.stalled.is_empty());
    }
}
