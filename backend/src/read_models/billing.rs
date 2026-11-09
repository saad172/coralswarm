//! Usage is an account-wide metering fact domain, separate from content scope.
use super::{dirty::DirtyTrigger, *};
use crate::routes::usage::*;
use std::collections::HashMap;
pub struct UsageSummary;
/// Internal calendar bucket: callers still send no usage query parameters.
/// A month rollover selects a fresh address, even on an entirely quiet account.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageParams {
    pub calendar_month: chrono::NaiveDate,
}
impl Default for UsageParams {
    fn default() -> Self {
        Self {
            calendar_month: month_start(Utc::now()).date_naive(),
        }
    }
}
impl Params for UsageParams {
    fn parse(
        path: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        if !path.is_empty() || !query.is_empty() {
            return Err("usage accepts no query parameters".into());
        }
        Ok(Self::default())
    }
}

impl ReadModel for UsageSummary {
    const NAME: &'static str = "usage_summary";
    const PATH: &'static str = "/usage/summary";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    const ACCOUNT_FACTS: bool = true;
    const ACCESS: access::AccessPolicy = access::AccessPolicy::Usage;
    type Params = UsageParams;
    type Payload = UsageSummaryResponse;
    fn graphs(_: &Viewer, _: &UsageParams) -> Vec<Uuid> {
        vec![]
    }
    fn compute<'a>(cx: ComputeCx<'a>, p: &'a UsageParams) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let tx = cx.control_tx.ok_or("usage requires control facts")?;
            let period_start = p
                .calendar_month
                .and_hms_opt(0, 0, 0)
                .ok_or("invalid usage month")?
                .and_utc();
            if month_start(period_start) != period_start {
                return Err("usage month is not canonical".into());
            }
            let sheet = crate::billing::rates::PriceSheet::load_tx(&mut **tx).await?;
            let conversion =
                crate::billing::credit_conversion::CreditConversionTable::load_tx(&mut **tx)
                    .await?;
            let usd_per_credit = conversion
                .active(period_start)
                .and_then(|r| r.reference_cost_per_credit.to_string().parse().ok());
            let balance_credits = sqlx::query_scalar::<_, f64>(
                "SELECT balance::float8 FROM credit_balances WHERE account_id=$1",
            )
            .bind(cx.account_id)
            .fetch_optional(&mut **tx)
            .await?
            .unwrap_or(0.);
            let plan = crate::billing::entitlement::current_plan(&mut **tx, cx.account_id)
                .await?
                .map(|r| PlanSummary {
                    code: r.code,
                    name: r.name,
                    credits: r.credits,
                    seats: r.seats,
                    credits_per_seat: r.credits_per_seat,
                    refresh_kind: r.refresh_kind,
                    granted_at: r.granted_at,
                });
            let ledger_coverage_start: Option<DateTime<Utc>> =
                sqlx::query_scalar("SELECT min(created_at) FROM credit_ledger WHERE account_id=$1")
                    .bind(cx.account_id)
                    .fetch_one(&mut **tx)
                    .await?;
            let ledger:Vec<(String,f64,f64)>=sqlx::query_as("SELECT entry_kind,(-credits)::float8,COALESCE(cost_usd,0)::float8 FROM credit_ledger WHERE account_id=$1 AND created_at >= $2 AND credits<0")
            .bind(cx.account_id).bind(period_start).fetch_all(&mut **tx).await?;
            let mut total_credits = 0.;
            let mut total_cost_usd = 0.;
            let mut axes: HashMap<String, (f64, f64, i64)> = HashMap::new();
            for (axis, credits, cost) in ledger {
                total_credits += credits;
                total_cost_usd += cost;
                let a = axes.entry(axis).or_default();
                a.0 += credits;
                a.1 += cost;
                a.2 += 1;
            }
            let mut by_axis: Vec<AxisSpend> = axes
                .into_iter()
                .map(|(axis, (credits, cost_usd, count))| AxisSpend {
                    axis,
                    credits,
                    cost_usd,
                    count,
                })
                .collect();
            by_axis.sort_by(|a, b| a.axis.cmp(&b.axis));
            let proof = match cx.blobs.and_then(|b| b.org_id()) {
                Some(org) => Some(
                    crate::services::usage_facts::MeteringProof::verify(
                        &mut **tx,
                        cx.account_id,
                        cx.compute_as,
                        org,
                    )
                    .await?,
                ),
                None => None,
            };
            let facts = match proof {
                Some(proof) => proof.facts(cx.tx, period_start).await?,
                None => {
                    crate::services::usage_facts::UsageFacts::personal(
                        cx.tx,
                        cx.account_id,
                        period_start,
                    )
                    .await?
                }
            };
            let mut by_graph: HashMap<Option<Uuid>, (Option<String>, f64)> = HashMap::new();
            for b in facts.buckets {
                let cost = estimate_cost_usd(
                    &sheet,
                    period_start,
                    &b.model,
                    b.input_tokens,
                    b.output_tokens,
                    b.cache_creation_tokens,
                    b.cache_read_tokens,
                );
                let entry = by_graph.entry(b.graph_id).or_insert((b.name, 0.));
                entry.1 += cost;
            }
            let mut by_workspace: Vec<WorkspaceSpend> = by_graph
                .into_iter()
                .map(|(graph_id, (name, cost_usd))| WorkspaceSpend {
                    graph_id,
                    name,
                    cost_usd,
                    unattributed: graph_id.is_none(),
                })
                .collect();
            by_workspace.sort_by(|a, b| {
                b.cost_usd
                    .partial_cmp(&a.cost_usd)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let ids: Vec<Uuid> = facts.askers.iter().filter_map(|r| r.user_id).collect();
            let emails: HashMap<Uuid, String> =
                sqlx::query_as::<_, (Uuid, String)>("SELECT id,email FROM users WHERE id=ANY($1)")
                    .bind(&ids)
                    .fetch_all(&mut **tx)
                    .await?
                    .into_iter()
                    .collect();
            let top_askers = facts
                .askers
                .into_iter()
                .map(|r| AskerStat {
                    user_id: r.user_id,
                    email: r.user_id.and_then(|u| emails.get(&u).cloned()),
                    asks: r.asks,
                })
                .collect();
            Ok(UsageSummaryResponse {
                period_start,
                period_end: cx.now,
                balance_credits,
                plan,
                credit_conversion_usd_per_credit: usd_per_credit,
                spend: SpendSummary {
                    total_credits,
                    total_cost_usd,
                    by_axis,
                },
                by_workspace,
                top_askers,
                atoms_ingested: facts.atoms_ingested,
                has_ledger_history: ledger_coverage_start.is_some(),
                ledger_coverage_start,
            })
        })
    }
    fn empty(_: &UsageParams) -> Self::Payload {
        UsageSummaryResponse {
            period_start: DateTime::UNIX_EPOCH,
            period_end: DateTime::UNIX_EPOCH,
            balance_credits: 0.,
            plan: None,
            credit_conversion_usd_per_credit: None,
            spend: SpendSummary {
                total_credits: 0.,
                total_cost_usd: 0.,
                by_axis: vec![],
            },
            by_workspace: vec![],
            top_askers: vec![],
            atoms_ingested: 0,
            has_ledger_history: false,
            ledger_coverage_start: None,
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        &[
            DirtyTrigger::Table("llm_usage"),
            DirtyTrigger::Table("query_history"),
            DirtyTrigger::Table("atoms"),
            DirtyTrigger::Table("graphs"),
            DirtyTrigger::Event(dirty::DirtyEvent::BillingChanged),
            DirtyTrigger::Event(dirty::DirtyEvent::OrgAdminChanged),
        ]
    }
}
