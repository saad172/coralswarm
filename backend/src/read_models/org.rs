//! Org member administration. Eligibility is enforced before GET and refresh,
//! including when an unchanged graph key still addresses a warmed admin row.
use super::{dirty::DirtyTrigger, *};
use serde_json::json;
pub struct MembersAdmin;
#[derive(Debug, Serialize, Deserialize)]
pub struct MembersPayload {
    pub members: Vec<Value>,
    pub slots: Option<Value>,
    pub domains: Vec<Value>,
    pub join_requests: Vec<Value>,
    pub seat_tiers: Vec<Value>,
    pub auto_add: Option<Value>,
}
impl ReadModel for MembersAdmin {
    const NAME: &'static str = "members_admin";
    const PATH: &'static str = "/load/org/members";
    const VERSION: i32 = 1;
    const PER_USER: bool = true;
    const CONTROL_FACTS: bool = true;
    const ACCESS: access::AccessPolicy = access::AccessPolicy::OrgAdmin;
    type Params = NoParams;
    type Payload = MembersPayload;
    fn compute<'a>(cx: ComputeCx<'a>, _: &'a NoParams) -> ComputeFuture<'a, Self::Payload> {
        Box::pin(async move {
            #[cfg(feature = "e2e")]
            super::note_compute();
            let tx = cx.control_tx.ok_or("members require control facts")?;
            let org: String = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id=$1")
                .bind(cx.account_id)
                .fetch_one(&mut **tx)
                .await?;
            // Direct trait callers get the same current eligibility check as
            // registry/worker dispatch. Never rely on a warmed actor key.
            Self::ACCESS
                .compute(&mut **tx, cx.account_id, cx.compute_as, Some(&org))
                .await?;
            let members = crate::services::members_page::roster(tx, cx.account_id, &org).await?;
            let slots = serde_json::to_value(
                crate::routes::org_slots::slot_pool_response(&mut **tx, cx.account_id).await?,
            )?;
            let domains = crate::services::org_domains::list_domains_tx(&mut **tx, cx.account_id)
                .await?
                .into_iter()
                .map(|d| {
                    serde_json::to_value(crate::routes::org_domains::OrgDomainResponse::from(d))
                })
                .collect::<Result<_, _>>()?;
            let mut requests =
                crate::services::org_join_requests::list_open_tx(&mut **tx, cx.account_id).await?;
            requests.extend(
                crate::services::org_join_requests::list_recent_decided_tx(
                    &mut **tx,
                    cx.account_id,
                    cx.now,
                    14,
                )
                .await?,
            );
            // Existing UI contract: all open rows oldest first, followed by
            // the last 14 days of decisions newest first. The stable key and
            // normal MAX_DELAY age/refresh policy expire this rolling window.
            let join_requests = requests
                .into_iter()
                .map(|r| {
                    serde_json::to_value(
                        crate::routes::org_join_requests::AdminJoinRequestItem::from(r),
                    )
                })
                .collect::<Result<_, _>>()?;
            let seat_tiers: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('code',code,'credits',credits::float8,'price_usd_cents',price_usd_cents) FROM seat_tiers WHERE is_active ORDER BY code")
                .fetch_all(&mut **tx).await?;
            let settings =
                crate::services::org_auto_add::read_settings_tx(&mut **tx, cx.account_id).await?;
            let tier: Option<String> = match settings.default_seat_tier_id {
                Some(id) => {
                    sqlx::query_scalar("SELECT code FROM seat_tiers WHERE id=$1")
                        .bind(id)
                        .fetch_optional(&mut **tx)
                        .await?
                }
                None => None,
            };
            Ok(MembersPayload {
                members,
                slots: Some(slots),
                domains,
                join_requests,
                seat_tiers,
                auto_add: Some(
                    json!({"enabled":settings.enabled,"tier":tier.unwrap_or_else(||"standard".into())}),
                ),
            })
        })
    }
    fn empty(_: &NoParams) -> Self::Payload {
        MembersPayload {
            members: vec![],
            slots: None,
            domains: vec![],
            join_requests: vec![],
            seat_tiers: vec![],
            auto_add: None,
        }
    }
    fn dirty_triggers() -> &'static [DirtyTrigger] {
        &[
            DirtyTrigger::Event(dirty::DirtyEvent::OrgAdminChanged),
            DirtyTrigger::Event(dirty::DirtyEvent::BillingChanged),
            DirtyTrigger::Event(dirty::DirtyEvent::ConnectorGrantChanged),
        ]
    }
}
