use chrono::{DateTime, Utc};
use sqlx::{types::Decimal, PgConnection};
use uuid::Uuid;

use super::funding_policy::db::{debit_key, write_extra_debit, ExtraDebit, PeriodSel, SettlementPlanner};

#[derive(Clone, Debug)]
pub(crate) struct FundingRequest {
    pub plane: String,
    pub receipt: Uuid,
    pub account: Uuid,
    pub payer: Uuid,
    pub graph: Uuid,
    pub logical_call: Uuid,
    pub request_hash: String,
    pub bound_micro_usd: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct FundingBinding {
    pub request: FundingRequest,
    pub bound_credits: Decimal,
    pub member_period_id: Option<Uuid>,
    pub conversion_effective_from: DateTime<Utc>,
    pub reference_cost_per_credit: Decimal,
    pub funding_basis: String,
}

#[derive(sqlx::FromRow)]
struct StoredReservation {
    pub plane: String,
    pub receipt_id: Uuid,
    pub account_id: Uuid,
    pub payer_id: Uuid,
    pub graph_id: Uuid,
    pub logical_call: Uuid,
    pub request_hash: String,
    pub bound_micro_usd: i64,
    pub bound_credits: Option<Decimal>,
    pub member_period_id: Option<Uuid>,
    pub conversion_effective_from: Option<DateTime<Utc>>,
    pub reference_cost_per_credit: Option<Decimal>,
    pub funding_basis: Option<String>,
    pub state: String,
    pub settled_micro_usd: Option<i64>,
}

pub(crate) enum ReserveOutcome {
    Held(FundingBinding),
    Exhausted,
    Unfunded,
    LegacyPending,
    Unavailable,
}

#[derive(Clone, Debug)]
pub(crate) struct TerminalFunding {
    pub request: FundingRequest,
    pub charged_micro_usd: Option<i64>,
}

fn invalid(message: &str) -> sqlx::Error {
    sqlx::Error::Protocol(message.to_owned())
}

async fn deadlines(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT set_config('lock_timeout','2s',true),set_config('statement_timeout','5s',true)",
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Takes the account's financial lock with this transaction's waits bounded
/// first. Unbounded, an admission queued behind the lock holds its control
/// connection for as long as the holder runs.
pub(crate) async fn enter_account_tx(conn: &mut PgConnection, account: Uuid) -> Result<(), sqlx::Error> {
    deadlines(&mut *conn).await?;
    super::renewal::lock_period_rotation(conn, account).await
}

impl StoredReservation {
    fn same_call(&self, request: &FundingRequest) -> bool {
        self.plane == request.plane
            && self.account_id == request.account
            && self.payer_id == request.payer
            && self.graph_id == request.graph
            && self.logical_call == request.logical_call
            && self.request_hash == request.request_hash
            && self.bound_micro_usd == request.bound_micro_usd
    }

    fn matches(&self, request: &FundingRequest) -> bool {
        self.plane == request.plane
            && self.receipt_id == request.receipt
            && self.account_id == request.account
            && self.payer_id == request.payer
            && self.graph_id == request.graph
            && self.logical_call == request.logical_call
            && self.request_hash == request.request_hash
            && self.bound_micro_usd == request.bound_micro_usd
    }

    /// The terminal financial fact for `charged` (`None`: released unsent)
    /// is already recorded for this exact call, so settling or releasing it
    /// again changes nothing.
    fn already_applied(&self, request: &FundingRequest, charged: Option<i64>) -> bool {
        self.matches(request)
            && match charged {
                Some(cost) => {
                    cost >= 0 && self.state == "settled" && self.settled_micro_usd == Some(cost)
                }
                None => self.state == "released",
            }
    }

    fn funded(self, request: &FundingRequest) -> Result<FundingBinding, sqlx::Error> {
        Ok(FundingBinding {
            request: request.clone(),
            bound_credits: self
                .bound_credits
                .filter(|v| *v > Decimal::ZERO)
                .ok_or_else(|| invalid("held reservation has no positive bound"))?,
            member_period_id: self.member_period_id,
            conversion_effective_from: self
                .conversion_effective_from
                .ok_or_else(|| invalid("held reservation has no conversion version"))?,
            reference_cost_per_credit: self
                .reference_cost_per_credit
                .filter(|v| *v > Decimal::ZERO)
                .ok_or_else(|| invalid("held reservation has no positive conversion"))?,
            funding_basis: self
                .funding_basis
                .filter(|basis| {
                    basis == "pool"
                        || basis == "extra"
                        || (basis == "allowance" && self.member_period_id.is_some())
                })
                .ok_or_else(|| invalid("held reservation has no funding basis"))?,
        })
    }
}

async fn read_tx(
    conn: &mut PgConnection,
    plane: &str,
    receipt: Uuid,
) -> Result<Option<StoredReservation>, sqlx::Error> {
    sqlx::query_as(
        "SELECT plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash, \
                bound_micro_usd,bound_credits,member_period_id,conversion_effective_from, \
                reference_cost_per_credit,funding_basis,state,settled_micro_usd \
           FROM managed_credit_reservations WHERE plane=$1 AND receipt_id=$2",
    )
    .bind(plane)
    .bind(receipt)
    .fetch_optional(conn)
    .await
}

/// The receipts among `terminals` whose terminal financial fact is already
/// recorded exactly, read in one round trip. [`settle_tx`] and
/// [`release_unsent_tx`] would return `Ok(false)` for each of them, so a
/// caller replaying settled history skips them instead of paying three
/// round trips per receipt under the account's financial lock. Any other
/// receipt (held, missing, or conflicting) still goes through the per-receipt
/// path, which applies or refuses it.
///
/// Sets the financial deadlines itself: on a page where every receipt is
/// skipped, no per-receipt call runs to set them, and the caller holds the
/// account's financial lock.
pub(crate) async fn already_applied_tx<'t>(
    conn: &mut PgConnection,
    terminals_in: impl IntoIterator<Item = &'t TerminalFunding>,
) -> Result<std::collections::HashSet<Uuid>, sqlx::Error> {
    let mut terminals: std::collections::HashMap<Uuid, Option<&TerminalFunding>> =
        Default::default();
    for terminal in terminals_in {
        terminals
            .entry(terminal.request.receipt)
            .and_modify(|seen| *seen = None)
            .or_insert(Some(terminal));
    }
    // A receipt named twice is never skipped: the per-receipt path decides it.
    terminals.retain(|_, terminal| terminal.is_some());
    if terminals.is_empty() {
        return Ok(Default::default());
    }
    deadlines(&mut *conn).await?;
    let receipts: Vec<Uuid> = terminals.keys().copied().collect();
    let stored: Vec<StoredReservation> = sqlx::query_as(
        "SELECT plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash, \
                bound_micro_usd,bound_credits,member_period_id,conversion_effective_from, \
                reference_cost_per_credit,funding_basis,state,settled_micro_usd \
           FROM managed_credit_reservations WHERE receipt_id=ANY($1)",
    )
    .bind(&receipts)
    .fetch_all(conn)
    .await?;
    Ok(stored
        .into_iter()
        .filter(|binding| {
            terminals
                .get(&binding.receipt_id)
                .copied()
                .flatten()
                .is_some_and(|terminal| {
                    binding.already_applied(&terminal.request, terminal.charged_micro_usd)
                })
        })
        .map(|binding| binding.receipt_id)
        .collect())
}

pub(crate) async fn existing_payer(
    control: &sqlx::PgPool,
    plane: &str,
    account: Uuid,
    logical_call: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| invalid(&e.to_string()))?;
    deadlines(&mut tx).await?;
    let payer = sqlx::query_scalar(
        "SELECT payer_id FROM managed_credit_reservations \
          WHERE plane=$1 AND account_id=$2 AND logical_call=$3 ORDER BY created_at,receipt_id LIMIT 1",
    ).bind(plane).bind(account).bind(logical_call).fetch_optional(&mut *tx).await?;
    tx.commit().await?;
    Ok(payer)
}

pub(crate) async fn reserve_tx(
    conn: &mut PgConnection,
    request: &FundingRequest,
    content: crate::workers::billing_derivation::LegacyContent<'_>,
) -> Result<ReserveOutcome, sqlx::Error> {
    deadlines(conn).await?;
    super::renewal::lock_period_rotation(conn, request.account).await?;
    if crate::workers::billing_derivation::reconcile_legacy_managed_tx(
        conn, request.account, content,
    ).await?.pending_legacy {
        return Ok(ReserveOutcome::LegacyPending);
    }
    if request.bound_micro_usd <= 0 || request.request_hash.len() != 64 {
        return Ok(ReserveOutcome::Unavailable);
    }
    if let Some(binding) = read_tx(conn, &request.plane, request.receipt).await? {
        if !binding.matches(request) {
            return Err(invalid("managed financial binding mismatch"));
        }
        return Ok(if binding.state == "held" {
            ReserveOutcome::Held(binding.funded(request)?)
        } else {
            ReserveOutcome::Exhausted
        });
    }
    let previous: Option<StoredReservation> = sqlx::query_as(
        "SELECT plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash, \
                bound_micro_usd,bound_credits,member_period_id,conversion_effective_from, \
                reference_cost_per_credit,funding_basis,state,settled_micro_usd \
           FROM managed_credit_reservations WHERE plane=$1 AND account_id=$2 AND logical_call=$3 \
          ORDER BY (reference_cost_per_credit IS NOT NULL) DESC,created_at,receipt_id LIMIT 1",
    )
    .bind(&request.plane)
    .bind(request.account)
    .bind(request.logical_call)
    .fetch_optional(&mut *conn)
    .await?;
    if previous
        .as_ref()
        .is_some_and(|previous| !previous.same_call(request))
    {
        return Err(invalid("managed logical call financial binding mismatch"));
    }
    let pinned = previous
        .filter(|previous| previous.reference_cost_per_credit.is_some())
        .map(|previous| previous.funded(request))
        .transpose()?;
    let conversion: Option<(DateTime<Utc>, Decimal)> = if let Some(pinned) = &pinned {
        Some((
            pinned.conversion_effective_from,
            pinned.reference_cost_per_credit,
        ))
    } else {
        sqlx::query_as(
            "SELECT effective_from,reference_cost_per_credit FROM credit_conversion \
          WHERE effective_from<=clock_timestamp() ORDER BY effective_from DESC LIMIT 1",
        )
        .fetch_optional(&mut *conn)
        .await?
    };
    let Some((effective, rate)) = conversion.filter(|(_, rate)| *rate > Decimal::ZERO) else {
        return Ok(ReserveOutcome::Unavailable);
    };
    let Some(bound) = Decimal::new(request.bound_micro_usd, 6)
        .checked_div(rate)
        .filter(|v| *v > Decimal::ZERO)
    else {
        return Ok(ReserveOutcome::Unavailable);
    };
    let Some(funding) = super::enforcement::managed_funding_tx(
        conn,
        request.account,
        request.payer,
        bound,
        match &pinned {
            Some(binding) => {
                super::enforcement::ManagedAllocation::Pinned(binding.member_period_id)
            }
            None => super::enforcement::ManagedAllocation::Current,
        },
    )
    .await?
    else {
        return Ok(ReserveOutcome::Unfunded);
    };
    sqlx::query(
        "INSERT INTO managed_credit_reservations \
           (plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash, \
            bound_micro_usd,bound_credits,member_period_id,conversion_effective_from, \
            reference_cost_per_credit,funding_basis,extra_credits) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
    )
    .bind(&request.plane)
    .bind(request.receipt)
    .bind(request.account)
    .bind(request.payer)
    .bind(request.graph)
    .bind(request.logical_call)
    .bind(&request.request_hash)
    .bind(request.bound_micro_usd)
    .bind(bound)
    .bind(funding.member_period)
    .bind(effective)
    .bind(rate)
    .bind(funding.basis)
    .bind(funding.extra_credits)
    .execute(&mut *conn)
    .await?;
    Ok(ReserveOutcome::Held(
        read_tx(conn, &request.plane, request.receipt)
            .await?
            .ok_or_else(|| invalid("managed reservation missing after insert"))?
            .funded(request)?,
    ))
}

/// Re-reads, under the financial lock, the reservation [`reserve_tx`] already
/// committed for this receipt: the fresh funding check immediately before a
/// send is armed. Unlike [`reserve_tx`] it never creates a reservation and
/// does not replay legacy liabilities. `reserve_tx` found none pending for
/// this account moments earlier, and the sealed receipt history
/// (`managed_source_receipt_history`) cannot mint new ones.
pub(crate) async fn confirm_held_tx(
    conn: &mut PgConnection,
    request: &FundingRequest,
) -> Result<ReserveOutcome, sqlx::Error> {
    deadlines(conn).await?;
    super::renewal::lock_period_rotation(conn, request.account).await?;
    if request.bound_micro_usd <= 0 || request.request_hash.len() != 64 {
        return Ok(ReserveOutcome::Unavailable);
    }
    let Some(binding) = read_tx(conn, &request.plane, request.receipt).await? else {
        return Ok(ReserveOutcome::Unavailable);
    };
    if !binding.matches(request) {
        return Err(invalid("managed financial binding mismatch"));
    }
    Ok(if binding.state == "held" {
        ReserveOutcome::Held(binding.funded(request)?)
    } else {
        ReserveOutcome::Exhausted
    })
}

pub(crate) async fn settle_tx(
    conn: &mut PgConnection,
    request: &FundingRequest,
    charged_micro_usd: i64,
) -> Result<bool, sqlx::Error> {
    let mut planner = SettlementPlanner::new(request.account);
    settle_tx_with(conn, request, charged_micro_usd, &mut planner).await
}

/// [`settle_tx`] with the caller's planner, so a batch of settlements and
/// ordinary debits is split against one running view of the account.
///
/// The charge is split by [`FundingPolicy::decide`] under the account lock
/// taken here: the plan allowance bears what it can, extra usage the rest up
/// to the member cap, monthly limit and balance, and anything beyond that is
/// recorded as unfunded the way overshoot always was (it stays on the plan
/// ledger and drives the plan pool below zero). The reservation's stored
/// basis is only an admission hint; the split is always recomputed.
pub(crate) async fn settle_tx_with(
    conn: &mut PgConnection,
    request: &FundingRequest,
    charged_micro_usd: i64,
    planner: &mut SettlementPlanner,
) -> Result<bool, sqlx::Error> {
    deadlines(conn).await?;
    super::renewal::lock_period_rotation(conn, request.account).await?;
    let binding = read_tx(conn, &request.plane, request.receipt)
        .await?
        .ok_or_else(|| invalid("managed settlement has no financial binding"))?;
    if !binding.matches(request) || charged_micro_usd < 0 {
        return Err(invalid("managed settlement binding mismatch"));
    }
    if binding.already_applied(request, Some(charged_micro_usd)) {
        return Ok(false);
    }
    if binding.state != "held" {
        return Err(invalid(
            "managed settlement conflicts with terminal financial state",
        ));
    }
    let cost = Decimal::new(charged_micro_usd, 6);
    let rate = binding
        .reference_cost_per_credit
        .filter(|v| *v > Decimal::ZERO)
        .ok_or_else(|| invalid("managed settlement has no positive pinned conversion"))?;
    let credits = cost
        .checked_div(rate)
        .ok_or_else(|| invalid("managed conversion overflow"))?;
    if charged_micro_usd > 0 {
        let sel = PeriodSel::Pinned(binding.member_period_id);
        let split = planner.plan(&mut *conn, Some(binding.payer_id), sel, credits).await?;
        // What the plan ledger bears: the allowance part plus any unfunded
        // remainder. The extra-usage part is on `extra_usage_ledger`.
        let plan_credits = split.plan_credits();
        let inserted = sqlx::query(
            "INSERT INTO credit_ledger \
                (account_id,entry_kind,credits,source_receipt_id,graph_id,cost_usd,attributed_user_id) \
             VALUES($1,'usage_managed',$2,$3,$4,$5,$6) \
             ON CONFLICT (source_receipt_id) WHERE source_receipt_id IS NOT NULL DO NOTHING",
        )
        .bind(binding.account_id).bind(-plan_credits).bind(binding.receipt_id)
        .bind(binding.graph_id).bind(cost).bind(binding.payer_id)
        .execute(&mut *conn).await?.rows_affected();
        if inserted != 1 {
            return Err(invalid(
                "managed receipt already debited outside its financial binding",
            ));
        }
        sqlx::query(
            "INSERT INTO credit_balances(account_id,balance,managed_debited_credits,updated_at) \
             VALUES($1,-$2,$2,now()) ON CONFLICT(account_id) DO UPDATE SET \
                balance=credit_balances.balance-$2, \
                managed_debited_credits=credit_balances.managed_debited_credits+$2,updated_at=now()",
        ).bind(binding.account_id).bind(plan_credits).execute(&mut *conn).await?;
        if let Some(period) = binding.member_period_id {
            let updated = sqlx::query(
                "UPDATE member_allowance_periods SET consumed_credits=consumed_credits+$4, \
                    managed_consumed_credits=managed_consumed_credits+$4,updated_at=now() \
                  WHERE id=$1 AND account_id=$2 AND user_id=$3",
            )
            .bind(period)
            .bind(binding.account_id)
            .bind(binding.payer_id)
            .bind(plan_credits)
            .execute(&mut *conn)
            .await?
            .rows_affected();
            if updated != 1 {
                return Err(invalid("managed pinned member period disappeared"));
            }
        }
        planner.record(&split);
        write_extra_debit(
            &mut *conn,
            &ExtraDebit {
                account: binding.account_id,
                split: &split,
                idempotency_key: debit_key("receipt", binding.account_id, binding.receipt_id),
                source_job_id: None,
                note: split.reason.map(|r| r.code()),
            },
        )
        .await?;
    }
    sqlx::query(
        "UPDATE managed_credit_reservations SET state='settled',settled_micro_usd=$3, \
            settled_at=now() WHERE plane=$1 AND receipt_id=$2 AND state='held'",
    )
    .bind(&binding.plane)
    .bind(binding.receipt_id)
    .bind(charged_micro_usd)
    .execute(conn)
    .await?;
    Ok(true)
}

pub(crate) async fn release_unsent_tx(
    conn: &mut PgConnection,
    request: &FundingRequest,
) -> Result<bool, sqlx::Error> {
    deadlines(conn).await?;
    super::renewal::lock_period_rotation(conn, request.account).await?;
    let Some(binding) = read_tx(conn, &request.plane, request.receipt).await? else {
        sqlx::query(
            "INSERT INTO managed_credit_reservations \
                (plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash, \
                 bound_micro_usd,state,settled_micro_usd,settled_at) \
             VALUES($1,$2,$3,$4,$5,$6,$7,$8,'released',0,now())",
        )
        .bind(&request.plane)
        .bind(request.receipt)
        .bind(request.account)
        .bind(request.payer)
        .bind(request.graph)
        .bind(request.logical_call)
        .bind(&request.request_hash)
        .bind(request.bound_micro_usd)
        .execute(conn)
        .await?;
        return Ok(true);
    };
    if !binding.matches(request) {
        return Err(invalid("managed unsent release binding mismatch"));
    }
    if binding.already_applied(request, None) {
        return Ok(false);
    }
    if binding.state != "held" {
        return Err(invalid("managed unsent release conflicts with settlement"));
    }
    Ok(sqlx::query(
        "UPDATE managed_credit_reservations SET state='released',settled_micro_usd=0, \
            settled_at=now() WHERE plane=$1 AND receipt_id=$2 AND state='held'",
    )
    .bind(&request.plane)
    .bind(request.receipt)
    .execute(conn)
    .await?
    .rows_affected()
        == 1)
}
