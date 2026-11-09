use super::*;
use crate::billing::entry_queue::{self, EntryTurn};
use crate::billing::managed_reservation::{self as credit, FundingRequest, ReserveOutcome};
use crate::tenancy::TenantId;
use crate::workers::billing_derivation::LegacyContent;

/// Test seam: runs on the org path after phase 2 (the control reservation)
/// committed and before phase 3 (the fresh funding check and the arm), the
/// window where another actor can cancel the receipt or touch the
/// reservation. Scoped to the calling task.
#[cfg(test)]
pub(super) type PhaseHook =
    std::sync::Arc<dyn Fn(Uuid) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;
#[cfg(test)]
tokio::task_local! {
    pub(super) static BETWEEN_FUND_AND_ARM: PhaseHook;
}

fn unavailable(error: impl std::fmt::Display) -> Deny {
    tracing::warn!(%error, "managed funded dispatch unavailable");
    Deny::Unavailable
}

/// The plane label the reservation and receipt carry: the door's
/// resolution, never re-derived from the worker's tenant.
fn plane(gate: &ManagedGate<'_>) -> String {
    gate.permit.label()
}

fn request(gate: &ManagedGate<'_>, receipt: Uuid) -> FundingRequest {
    FundingRequest {
        plane: plane(gate),
        receipt,
        account: gate.call.account_id,
        payer: gate.call.principal,
        graph: gate.graph_job().0,
        logical_call: gate.logical_call,
        request_hash: gate.request_hash.clone(),
        bound_micro_usd: gate.worst.per_attempt.get(),
    }
}

/// An org call's entered control transaction, with the in-process turn that
/// was taken BEFORE its connection (#1497). Drop the turn right after the
/// transaction commits; the advisory lock goes with the commit.
type EnteredControl<'a> = (EntryTurn, Transaction<'a, Postgres>);

async fn control_tx<'a>(gate: &ManagedGate<'a>) -> Result<Option<EnteredControl<'a>>, Deny> {
    match gate.call.plane {
        JobScope::Consumer { .. } => Ok(None),
        JobScope::Org(_) => {
            let turn = entry_queue::wait_turn(gate.call.account_id).await.map_err(unavailable)?;
            let mut tx=begin_account_scoped(gate.call.control,gate.call.account_id).await.map_err(unavailable)?;
            credit::enter_account_tx(&mut tx,gate.call.account_id).await.map_err(unavailable)?;
            Ok(Some((turn, tx)))
        },
    }
}

fn split<'a>(entered: Option<EnteredControl<'a>>) -> (Option<EntryTurn>, Option<Transaction<'a, Postgres>>) {
    match entered {
        Some((turn, tx)) => (Some(turn), Some(tx)),
        None => (None, None),
    }
}

async fn fresh_cohort_fence(
    gate: &ManagedGate<'_>,
    content: &mut Transaction<'_, Postgres>,
    mut control: Option<&mut Transaction<'_, Postgres>>,
) -> Result<(), Deny> {
    // Exhaustive: a new subject does not compile until it has a fence, and
    // every fence is handed the door's permit to re-confirm.
    let permit = &gate.permit;
    match gate.call.subject {
        Subject::EmbeddingBatch { batch } => return batch.fresh_locked(content, control, permit).await,
        Subject::StandingWindow { work } => {
            let db: &mut sqlx::PgConnection = match control.as_mut() {
                Some(control) => &mut ***control,
                None => &mut **content,
            };
            permit.confirm_locked(db, work.account_id).await?;
            return crate::managed::standing::execution::attempt_allowed_locked(
                content, control, permit.tenant(), work,
            )
            .await
            .map_err(|_| Deny::Unavailable)?
            .then_some(())
            .ok_or(Deny::Hold(HoldCode::Fenced));
        }
        // Non-window path: no corpus run or unit owns a graph-level call.
        Subject::Enrichment { work } => {
            return work.fence_locked(content, control, permit, gate.call.principal).await
        }
        // An Ask call: the asker, the graph and the account policy.
        Subject::InteractiveAsk { work } => {
            return work.fence_locked(content, control, permit, gate.call.principal).await
        }
        Subject::Window { .. } => {}
    }
    let (graph, version, profile, _) = gate.window().ok_or(Deny::Unavailable)?;
    let owner: Option<(Uuid, Uuid, String, String)> = sqlx::query_as(
        "SELECT r.account_id,r.requested_by,r.status,r.run_kind \
           FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u \
             ON u.run_id=r.run_id AND u.graph_id=r.graph_id \
          WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 \
            AND u.extraction_version=$3 AND u.execution_owner FOR UPDATE OF r",
    )
    .bind(graph)
    .bind(version)
    .bind(profile)
    .fetch_optional(&mut **content)
    .await
    .map_err(unavailable)?;
    let Some((account, requester, status, kind)) = owner else {
        return Err(Deny::Hold(HoldCode::RequesterAccessLost));
    };
    if account != gate.call.account_id || kind != "cohort" {
        return Err(Deny::Hold(HoldCode::Fenced));
    }
    let lifecycle_ready = crate::corpus_enrichment::lifecycle_ready_tx(content)
        .await
        .map_err(unavailable)?;
    let source: Option<String> = sqlx::query_scalar(
        "SELECT s.access_state FROM source_items s JOIN source_versions v \
             ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id \
          WHERE v.graph_id=$1 AND v.version_id=$2 FOR SHARE OF s",
    )
    .bind(graph)
    .bind(version)
    .fetch_optional(&mut **content)
    .await
    .map_err(unavailable)?;
    let valid_version: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM source_versions WHERE graph_id=$1 AND version_id=$2 \
                       AND extraction_version=$3 FOR SHARE)",
    )
    .bind(graph)
    .bind(version)
    .bind(profile)
    .fetch_one(&mut **content)
    .await
    .map_err(unavailable)?;
    let db: &mut sqlx::PgConnection = match control.as_mut() {
        Some(control) => &mut ***control,
        None => &mut **content,
    };
    permit.confirm_locked(&mut *db, account).await?;
    let current: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2) \
            AND NOT EXISTS(SELECT 1 FROM org_revocations r JOIN accounts a \
              ON a.clerk_org_id=r.clerk_org_id WHERE a.id=$1)",
    )
    .bind(account)
    .bind(requester)
    .fetch_one(&mut *db)
    .await
    .map_err(unavailable)?;
    let reachable: bool = match permit.tenant() {
        TenantId::Consumer => sqlx::query_scalar("SELECT resolve_graph_reachable_by_user($1,$2)")
            .bind(graph)
            .bind(requester)
            .fetch_one(&mut *db)
            .await
            .map_err(unavailable)?,
        TenantId::Org(_) => {
            sqlx::query_scalar("SELECT resolve_tenant_graph_reachable_by_user($1,$2,$3)")
                .bind(graph)
                .bind(account)
                .bind(requester)
                .fetch_one(&mut **content)
                .await
                .map_err(unavailable)?
        }
    };
    let reason = if !current || !reachable {
        Some(HoldCode::RequesterAccessLost)
    } else if source.as_deref() != Some("permitted") || !valid_version {
        Some(HoldCode::SourceRemovedOrAccessLost)
    } else if status != "running" {
        Some(HoldCode::ManagedRunPaused)
    } else if !lifecycle_ready {
        Some(HoldCode::LifecycleContractNotReady)
    } else {
        None
    };
    match reason {
        Some(code) => Err(Deny::Hold(code)),
        None => Ok(()),
    }
}

async fn prepare_tx(
    gate: &ManagedGate<'_>,
    tx: &mut Transaction<'_, Postgres>,
    funding: &FundingRequest,
    attempt_no: u32,
) -> Result<(), Deny> {
    let (graph, job) = gate.graph_job();
    let period: chrono::NaiveDate =
        sqlx::query_scalar("SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date")
            .fetch_one(&mut **tx)
            .await
            .map_err(unavailable)?;
    sqlx::query(
        "INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) \
         VALUES($1,$2,$3,$4) ON CONFLICT(account_id,purpose,period_start) DO NOTHING",
    )
    .bind(funding.account)
    .bind(gate.purpose.as_str())
    .bind(period)
    .bind(gate.caps.daily(gate.purpose).get())
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    let reserved = sqlx::query(
        "UPDATE managed_budget_periods SET reserved_micro_usd=reserved_micro_usd+$4,attempts=attempts+1 \
          WHERE account_id=$1 AND purpose=$2 AND period_start=$3 \
            AND reserved_micro_usd+charged_micro_usd+$4<=limit_micro_usd",
    ).bind(funding.account).bind(gate.purpose.as_str()).bind(period).bind(funding.bound_micro_usd)
    .execute(&mut **tx).await.map_err(unavailable)?.rows_affected();
    if reserved != 1 {
        return Err(Deny::Hold(HoldCode::DailyBudgetExhausted));
    }
    let (unit, version) = if matches!(
        gate.call.subject,
        Subject::EmbeddingBatch { .. } | Subject::Enrichment { .. } | Subject::InteractiveAsk { .. }
    ) {
        // No per-version unit: the per-attempt cap (checked at open) and the
        // purpose's daily cap (above) are the bounds.
        (None, None)
    } else {
        let (_, version, profile, _) = gate.window().ok_or(Deny::Unavailable)?;
        let unit: Option<Uuid> = sqlx::query_scalar(
        "UPDATE corpus_enrichment_units SET charged_micro_usd=charged_micro_usd+$4 \
          WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 \
            AND extraction_version=$3 AND execution_owner AND charged_micro_usd+$4<=$5 RETURNING unit_id",
    ).bind(graph).bind(version).bind(profile).bind(funding.bound_micro_usd).bind(gate.caps.per_version.get())
    .fetch_optional(&mut **tx).await.map_err(unavailable)?;
        let Some(unit) = unit else {
            return Err(Deny::Refuse(RefuseCode::VersionCostCap));
        };
        (Some(unit), Some(version))
    };
    sqlx::query(
        "INSERT INTO managed_attempt_receipts \
            (receipt_id,account_id,purpose,period_start,graph_id,unit_id,source_version_id,job_id, \
             attributed_user_id,logical_call,attempt_no,candidate_models,input_bound_tokens, \
             max_tokens,reserved_micro_usd,protocol_version,dispatch_phase,content_plane,request_hash) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,1,'prepared',$16,$17)",
    ).bind(funding.receipt).bind(funding.account).bind(gate.purpose.as_str()).bind(period)
    .bind(graph).bind(unit).bind(version).bind(job).bind(funding.payer).bind(funding.logical_call)
    .bind(i16::try_from(attempt_no).map_err(unavailable)?).bind(&gate.worst.terms.models)
    .bind(gate.worst.input_bound_tokens as i64).bind(gate.worst.max_tokens as i32)
    .bind(funding.bound_micro_usd).bind(&funding.plane).bind(&funding.request_hash)
    .execute(&mut **tx).await.map_err(unavailable)?;
    if let Subject::EmbeddingBatch { batch } = gate.call.subject {
        batch.insert_inputs(tx, funding.receipt).await?;
    }
    if let Subject::Enrichment { work } = gate.call.subject {
        // All-input receipt: every input is durable before the send is armed.
        work.insert_inputs(tx, funding.receipt).await?;
    }
    Ok(())
}

async fn fund_tx(tx: &mut Transaction<'_, Postgres>, funding: &FundingRequest, content: LegacyContent<'_>) -> Result<(), Deny> {
    funded_outcome(credit::reserve_tx(tx, funding, content).await.map_err(unavailable)?)
}

async fn confirm_funded_tx(tx: &mut Transaction<'_, Postgres>, funding: &FundingRequest) -> Result<(), Deny> {
    funded_outcome(credit::confirm_held_tx(tx, funding).await.map_err(unavailable)?)
}

fn funded_outcome(outcome: ReserveOutcome) -> Result<(), Deny> {
    match outcome {
        ReserveOutcome::Held(_) => Ok(()),
        ReserveOutcome::Exhausted | ReserveOutcome::Unfunded => {
            Err(Deny::Hold(HoldCode::CreditsExhausted))
        }
        ReserveOutcome::LegacyPending => Err(Deny::Hold(HoldCode::LegacyManagedLiabilitiesPending)),
        ReserveOutcome::Unavailable => Err(Deny::Unavailable),
    }
}

async fn arm_tx(tx: &mut Transaction<'_, Postgres>, receipt: Uuid) -> Result<(), Deny> {
    let armed: bool = sqlx::query_scalar("SELECT managed_arm_v1($1)")
        .bind(receipt)
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)?;
    if !armed {
        return Err(Deny::Unavailable);
    }
    Ok(())
}

/// Owned copy of what an unsent cancellation needs, so it can outlive a
/// dropped admission.
struct UnsentCleanup {
    plane: OwnedPlane,
    control: sqlx::PgPool,
    funding: FundingRequest,
}

enum OwnedPlane {
    Consumer { pool: sqlx::PgPool, account_id: Uuid },
    Org(sqlx::PgPool),
}

impl UnsentCleanup {
    fn new(gate: &ManagedGate<'_>, funding: &FundingRequest) -> Self {
        Self {
            plane: match gate.call.plane {
                JobScope::Consumer { pool, account_id } => OwnedPlane::Consumer {
                    pool: (*pool).clone(),
                    account_id: *account_id,
                },
                JobScope::Org(pool) => OwnedPlane::Org((*pool).clone()),
            },
            control: gate.call.control.clone(),
            funding: funding.clone(),
        }
    }

    /// Cancels the receipt if it is still unsent, then releases whatever
    /// funding its terminal outcome leaves held. Idempotent: an armed receipt
    /// is never cancelled, and a released reservation is not released again.
    async fn run(&self) -> Result<(), Deny> {
        let plane = match &self.plane {
            OwnedPlane::Consumer { pool, account_id } => JobScope::Consumer {
                pool,
                account_id: *account_id,
            },
            OwnedPlane::Org(pool) => JobScope::Org(pool),
        };
        let mut content = begin_plane(&plane).await.map_err(unavailable)?;
        sqlx::query_scalar::<_, bool>("SELECT managed_cancel_unsent_v1($1)")
            .bind(self.funding.receipt)
            .fetch_one(&mut *content)
            .await
            .map_err(unavailable)?;
        content.commit().await.map_err(unavailable)?;
        reconcile_funding(&plane, &self.control, &self.funding)
            .await
            .map_err(unavailable)
    }
}

/// Cleans up after a `reserve` whose caller dropped it before it returned
/// (a timeout during a lock wait). Without it the receipt stays `prepared`,
/// and the control reservation held, until the 10-minute reaper. The
/// dropped future's open transactions roll back as their connections
/// return, so the cleanup retries briefly past their row locks.
struct AbandonGuard(Option<UnsentCleanup>);

impl AbandonGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for AbandonGuard {
    fn drop(&mut self) {
        let Some(cleanup) = self.0.take() else { return };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(receipt=%cleanup.funding.receipt,
                "dropped managed admission left for durable recovery: no runtime to cancel it");
            return;
        };
        runtime.spawn(async move {
            let mut last = None;
            for delay_ms in [0u64, 250, 1000, 3000] {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                match cleanup.run().await {
                    Ok(()) => return,
                    Err(error) => last = Some(error),
                }
            }
            tracing::warn!(error=?last, receipt=%cleanup.funding.receipt,
                "dropped managed admission cancellation pending durable recovery");
        });
    }
}

pub(super) async fn reserve(gate: &ManagedGate<'_>, attempt_no: u32) -> Result<Reservation, Deny> {
    if gate.caps.disabled {
        return Err(gate.hold(HoldCode::ProcessingDisabled).await);
    }
    let funding = request(gate, Uuid::new_v4());
    let mut abandoned = AbandonGuard(Some(UnsentCleanup::new(gate, &funding)));
    let result = reserve_inner(gate, &funding, attempt_no).await;
    if result.is_err() {
        let cancelled = async {
            let mut content = begin_plane(gate.call.plane).await.map_err(unavailable)?;
            sqlx::query_scalar::<_, bool>("SELECT managed_cancel_unsent_v1($1)")
                .bind(funding.receipt)
                .fetch_one(&mut *content)
                .await
                .map_err(unavailable)?;
            content.commit().await.map_err(unavailable)?;
            reconcile(gate, funding.receipt).await.map_err(unavailable)
        }
        .await;
        if let Err(error) = cancelled {
            tracing::warn!(?error, receipt=%funding.receipt, "managed cancellation pending durable recovery");
        }
    }
    abandoned.disarm();
    match result {
        Err(Deny::Hold(code)) => Err(gate.hold(code).await),
        other => other,
    }
}

async fn reserve_inner(
    gate: &ManagedGate<'_>,
    funding: &FundingRequest,
    attempt_no: u32,
) -> Result<Reservation, Deny> {
    // Every financial transaction below takes its in-process turn
    // (`entry_queue`) BEFORE the control connection it will enter on, and
    // gives the turn back the moment that transaction commits. On the
    // consumer plane the content transaction IS the control connection, so
    // the turn comes before `begin_plane` there too.
    let (mut turn, mut control) = split(control_tx(gate).await?);
    if control.is_none() {
        turn = Some(entry_queue::wait_turn(funding.account).await.map_err(unavailable)?);
    }
    let mut content = begin_plane(gate.call.plane).await.map_err(unavailable)?;
    if control.is_none() {
        credit::enter_account_tx(&mut content,funding.account).await.map_err(unavailable)?;
    }
    fresh_cohort_fence(gate, &mut content, control.as_mut()).await?;
    if control.is_none() {
        fund_tx(&mut content, funding, LegacyContent::Control).await?;
        prepare_tx(gate, &mut content, funding, attempt_no).await?;
        arm_tx(&mut content, funding.receipt).await?;
        content.commit().await.map_err(unavailable)?;
        drop(turn);
    } else {
        prepare_tx(gate, &mut content, funding, attempt_no).await?;
        control
            .take()
            .expect("org control transaction")
            .commit()
            .await
            .map_err(unavailable)?;
        drop(turn.take());
        content.commit().await.map_err(unavailable)?;
        let turn = entry_queue::wait_turn(funding.account).await.map_err(unavailable)?;
        let mut financial = begin_account_scoped(gate.call.control, funding.account)
            .await
            .map_err(unavailable)?;
        credit::enter_account_tx(&mut financial,funding.account).await.map_err(unavailable)?;
        let mut content = begin_plane(gate.call.plane).await.map_err(unavailable)?;
        let crate::managed::LiabilityPlane::Org(org)=gate.permit.plane() else { return Err(Deny::Unavailable); };
        fund_tx(&mut financial, funding, LegacyContent::Org { content:&mut content,org }).await?;
        content.commit().await.map_err(unavailable)?;
        financial.commit().await.map_err(unavailable)?;
        drop(turn);
        #[cfg(test)]
        if let Ok(hook) = BETWEEN_FUND_AND_ARM.try_with(|hook| hook.clone()) {
            hook(funding.receipt).await;
        }
        let (turn, mut control) = split(control_tx(gate).await?);
        let mut content = begin_plane(gate.call.plane).await.map_err(unavailable)?;
        fresh_cohort_fence(gate, &mut content, control.as_mut()).await?;
        confirm_funded_tx(control.as_mut().expect("org control transaction"), funding).await?;
        arm_tx(&mut content, funding.receipt).await?;
        control
            .take()
            .expect("org control transaction")
            .commit()
            .await
            .map_err(unavailable)?;
        drop(turn);
        content.commit().await.map_err(unavailable)?;
    }
    Ok(Reservation {
        receipt_id: Some(funding.receipt),
    })
}

pub(super) async fn reconcile(
    gate: &ManagedGate<'_>,
    receipt: Uuid,
) -> Result<(), crate::store::StoreError> {
    reconcile_funding(gate.call.plane, gate.call.control, &request(gate, receipt)).await
}

async fn reconcile_funding(
    plane: &JobScope<'_>,
    control_pool: &sqlx::PgPool,
    funding: &FundingRequest,
) -> Result<(), crate::store::StoreError> {
    let receipt = funding.receipt;
    // Consumer plane: `content` is a control connection and the settlement
    // below enters the financial lock on it, so the turn comes first.
    let turn = match plane {
        JobScope::Consumer { .. } => Some(entry_queue::wait_turn(funding.account).await?),
        JobScope::Org(_) => None,
    };
    let mut content = begin_plane(plane).await?;
    let terminal: Option<(String, i64)> = sqlx::query_as(
        "SELECT dispatch_phase,settled_micro_usd FROM managed_attempt_receipts \
          WHERE receipt_id=$1 AND protocol_version=1 AND state<>'reserved' \
            AND account_id=$2 AND content_plane=$3 AND request_hash=$4",
    )
    .bind(receipt)
    .bind(funding.account)
    .bind(&funding.plane)
    .bind(&funding.request_hash)
    .fetch_optional(&mut *content)
    .await?;
    if let Some((phase, cost)) = terminal {
        if matches!(plane, JobScope::Consumer { .. }) {
            if phase == "cancelled_unsent" {
                credit::release_unsent_tx(&mut content, funding).await?;
            } else {
                credit::settle_tx(&mut content, funding, cost).await?;
            }
            content.commit().await?;
        } else {
            content.commit().await?;
            let turn = entry_queue::wait_turn(funding.account).await?;
            let mut control = begin_account_scoped(control_pool, funding.account).await?;
            if phase == "cancelled_unsent" {
                credit::release_unsent_tx(&mut control, funding).await?;
            } else {
                credit::settle_tx(&mut control, funding, cost).await?;
            }
            control.commit().await?;
            drop(turn);
        }
    }
    drop(turn);
    Ok(())
}

pub(crate) async fn recover_logical_call(
    plane: &JobScope<'_>,
    control: &sqlx::PgPool,
    tenant: &TenantId,
    account: Uuid,
    logical_call: Uuid,
) -> Result<(), crate::store::StoreError> {
    let expected_plane = match tenant {
        TenantId::Consumer => "consumer".to_owned(),
        TenantId::Org(org) => format!("org:{org}"),
    };
    let mut content = begin_plane(plane).await?;
    let preacquired_control = if matches!(tenant, TenantId::Org(_)) {
        Some(begin_account_scoped(control, account).await?)
    } else {
        None
    };
    let receipts: Vec<Uuid> = sqlx::query_scalar(
        "SELECT receipt_id FROM managed_attempt_receipts \
          WHERE protocol_version=1 AND content_plane=$1 AND account_id=$2 AND logical_call=$3 \
          ORDER BY account_id,purpose,period_start,unit_id,receipt_id",
    )
    .bind(&expected_plane)
    .bind(account)
    .bind(logical_call)
    .fetch_all(&mut *content)
    .await?;
    for receipt in &receipts {
        sqlx::query_scalar::<_, bool>("SELECT managed_cancel_unsent_v1($1)")
            .bind(receipt)
            .fetch_one(&mut *content)
            .await?;
        sqlx::query_scalar::<_, bool>(
            "SELECT managed_settle_v1($1,'ambiguous',NULL,NULL,NULL,NULL)",
        )
        .bind(receipt)
        .fetch_one(&mut *content)
        .await?;
    }
    #[derive(sqlx::FromRow)]
    struct TerminalReceipt {
        receipt_id: Uuid,
        attributed_user_id: Uuid,
        graph_id: Uuid,
        request_hash: String,
        reserved_micro_usd: i64,
        settled_micro_usd: i64,
        dispatch_phase: String,
    }
    let terminals: Vec<TerminalReceipt> = sqlx::query_as(
        "SELECT receipt_id,attributed_user_id,graph_id,request_hash,reserved_micro_usd, \
                settled_micro_usd,dispatch_phase FROM managed_attempt_receipts \
          WHERE protocol_version=1 AND content_plane=$1 AND account_id=$2 AND logical_call=$3 \
            AND state<>'reserved' ORDER BY receipt_id",
    )
    .bind(&expected_plane)
    .bind(account)
    .bind(logical_call)
    .fetch_all(&mut *content)
    .await?;
    if terminals.len() != receipts.len() {
        return Err(
            sqlx::Error::Protocol("managed recovery left an unresolved receipt".into()).into(),
        );
    }
    if let Some(control) = preacquired_control {
        control.commit().await?;
    }
    content.commit().await?;
    // Both connections above are back in their pools: the turn is taken
    // holding nothing, as every entry-queue caller must.
    let turn = entry_queue::wait_turn(account).await?;
    let mut financial = begin_account_scoped(control, account).await?;
    for terminal in terminals {
        let request = FundingRequest {
            plane: expected_plane.clone(),
            receipt: terminal.receipt_id,
            account,
            payer: terminal.attributed_user_id,
            graph: terminal.graph_id,
            logical_call,
            request_hash: terminal.request_hash,
            bound_micro_usd: terminal.reserved_micro_usd,
        };
        if terminal.dispatch_phase == "cancelled_unsent" {
            credit::release_unsent_tx(&mut financial, &request).await?;
        } else {
            credit::settle_tx(&mut financial, &request, terminal.settled_micro_usd).await?;
        }
    }
    financial.commit().await?;
    drop(turn);
    Ok(())
}
