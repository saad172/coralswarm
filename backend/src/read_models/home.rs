//! Stored Home sections. All content work runs only in the refresher snapshot.
use super::{dirty::DirtyTrigger, *};
use crate::services::{home_metrics as hm, workspace_activity as wa};
use chrono::{Duration, NaiveDate};
use chrono_tz::Tz;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HomeMetricsParams {
    pub days: u32,
    pub tz: String,
    /// Bounded calendar identity; never an arbitrary request timestamp.
    pub calendar_day: NaiveDate,
}
impl Params for HomeMetricsParams {
    fn parse(
        path: BTreeMap<String, String>,
        mut q: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        if !path.is_empty() {
            return Err("unknown path parameter".into());
        }
        let days = q.remove("days").map(|s| s.parse()).transpose()?;
        let tz = q.remove("tz");
        if !q.is_empty() {
            return Err("unknown query parameter".into());
        }
        let (days, tz) = hm::parse_params(days, tz.as_deref()).map_err(|e| e.message())?;
        Ok(Self {
            days,
            tz: tz.name().into(),
            calendar_day: Utc::now().with_timezone(&tz).date_naive(),
        })
    }
}
pub struct HomeMetrics;
impl ReadModel for HomeMetrics {
    const NAME: &'static str = "home_metrics";
    const PATH: &'static str = "/home/metrics";
    const VERSION: i32 = 1;
    // questions_asked is the actor's own history even with full graph history.
    const PER_USER: bool = true;
    type Params = HomeMetricsParams;
    type Payload = Value;
    fn enabled() -> bool {
        hm::enabled()
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let tz: Tz = p.tz.parse()?;
            let end = crate::workers::journal_sweep::resolve_local_midnight(
                tz,
                p.calendar_day
                    .succ_opt()
                    .ok_or("invalid calendar day")?
                    .and_hms_opt(0, 0, 0)
                    .ok_or("invalid calendar midnight")?,
            );
            let now = cx.now.min(end - Duration::nanoseconds(1));
            let payload = hm::compute(
                &mut **cx.tx,
                cx.account_id,
                cx.compute_as,
                cx.graphs,
                p.days,
                tz,
                now,
            )
            .await?;
            Ok(serde_json::to_value(payload)?)
        })
    }
    fn empty(p: &Self::Params) -> Value {
        let tz = p.tz.parse().unwrap_or(Tz::UTC);
        let at = crate::workers::journal_sweep::resolve_local_midnight(
            tz,
            p.calendar_day
                .and_hms_opt(0, 0, 0)
                .expect("calendar midnight"),
        );
        serde_json::to_value(hm::assemble(
            &hm::Window::new(at, p.days, tz),
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
        ))
        .expect("Home metrics serialization")
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        HOME_TRIGGERS
    }
}
const HOME_TRIGGERS: &[DirtyTrigger] = &[
    DirtyTrigger::Table("atoms"),
    DirtyTrigger::Table("nodes"),
    DirtyTrigger::Table("reef_identity_corrections"),
    DirtyTrigger::Table("graphs"),
    DirtyTrigger::Table("node_atoms"),
    DirtyTrigger::Table("node_semantic_atoms"),
    DirtyTrigger::Table("source_items"),
    DirtyTrigger::Table("source_versions"),
    DirtyTrigger::Table("source_version_chunks"),
    DirtyTrigger::Table("semantic_atoms"),
    DirtyTrigger::Table("query_history"),
    DirtyTrigger::Table("coding_sessions"),
];
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceActivityParams {
    pub limit: u32,
    pub since_days: u32,
    pub calendar_day: NaiveDate,
}
impl Params for WorkspaceActivityParams {
    fn parse(
        path: BTreeMap<String, String>,
        mut q: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        if !path.is_empty() {
            return Err("unknown path parameter".into());
        }
        let limit = q.remove("limit").map(|s| s.parse()).transpose()?;
        let since = q.remove("since_days").map(|s| s.parse()).transpose()?;
        if !q.is_empty() {
            return Err("unknown query parameter".into());
        }
        let (limit, since_days) = wa::parse_params(limit, since).map_err(|e| e.message())?;
        Ok(Self {
            limit,
            since_days,
            calendar_day: Utc::now().date_naive(),
        })
    }
}
pub struct WorkspaceActivity;
impl ReadModel for WorkspaceActivity {
    const NAME: &'static str = "workspace_activity";
    const PATH: &'static str = "/home/workspace-activity";
    const VERSION: i32 = 1;
    type Params = WorkspaceActivityParams;
    type Payload = Value;
    fn enabled() -> bool {
        wa::enabled()
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a Self::Params) -> ComputeFuture<'a, Value> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let end = p
                .calendar_day
                .succ_opt()
                .ok_or("invalid calendar day")?
                .and_hms_opt(0, 0, 0)
                .ok_or("invalid calendar midnight")?
                .and_utc();
            let now = cx.now.min(end - Duration::nanoseconds(1));
            Ok(serde_json::to_value(
                wa::compute(
                    &mut **cx.tx,
                    cx.account_id,
                    cx.graphs,
                    p.limit,
                    p.since_days,
                    now,
                )
                .await?,
            )?)
        })
    }
    fn empty(p: &Self::Params) -> Value {
        serde_json::to_value(wa::assemble(
            &[],
            &[],
            &[],
            Utc::now() - Duration::days(p.since_days as i64),
            p.limit as usize,
        ))
        .expect("Workspace activity serialization")
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        HOME_TRIGGERS
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn params_are_canonical_and_reject_actor_injection() {
        let p = HomeMetricsParams::parse(BTreeMap::new(), BTreeMap::new()).unwrap();
        assert_eq!(p.days, 30);
        assert_eq!(p.tz, "UTC");
        assert!(HomeMetricsParams::parse(
            BTreeMap::new(),
            [("user_id".into(), Uuid::new_v4().to_string())].into()
        )
        .is_err());
        assert!(WorkspaceActivityParams::parse(
            BTreeMap::new(),
            [("limit".into(), "0".into())].into()
        )
        .is_err());
    }
}
