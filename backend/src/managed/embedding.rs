mod operation;
pub(super) use operation::PreparedBatch;
pub(in crate::managed) use operation::{admit_locked, begin, executor, member_reaches_locked, policy};
pub(crate) use operation::{process, process_statements, EmbeddingReport};
pub(crate) use operation::{failure_cause, settle_unprocessed, with_failure_cause, Unprocessed};
#[cfg(test)]
pub(crate) use operation::recover_owned_job_for_test;

use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
};

use chrono::{DateTime, Utc};
use coralswarm_core::send_gate::{
    Deny, GateFuture, HoldCode, MaxPrice, RefuseCode, Reservation, SendGate, SendOutcome,
    SendTerms, Sent,
};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{Caps, ManagedPurpose, MicroUsd, PlanePermit, WorstCase};
use crate::{
    billing::rates::PriceSheet,
    tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
    workers::embed::JobScope,
};

const MODEL: &str = "openai/text-embedding-3-small";

#[derive(Debug, thiserror::Error)]
enum EmbeddingError {
    #[error("invalid embedding input batch")]
    InvalidBatch,
    #[error("embedding input scope or revision changed")]
    Stale,
    #[error("embedding refused: {0:?}")]
    Refused(RefuseCode),
    #[error("embedding daily budget exhausted")]
    DailyBudget,
    #[error(transparent)]
    Storage(#[from] sqlx::Error),
}

struct ReceiptInput {
    atom_id: Uuid,
    content_revision: i64,
    text: String,
}

struct ReceiptBatch {
    account_id: Uuid,
    graph_id: Uuid,
    job_id: Uuid,
    attributed_user_id: Uuid,
    inputs: Vec<ReceiptInput>,
}

impl ReceiptBatch {
    fn checked(
        account_id: Uuid,
        graph_id: Uuid,
        job_id: Uuid,
        attributed_user_id: Uuid,
        inputs: Vec<ReceiptInput>,
    ) -> Result<Self, EmbeddingError> {
        Self::checked_up_to(account_id, graph_id, job_id, attributed_user_id, inputs, 8)
    }

    /// [`Self::checked`] with a larger batch: statement batches hold up to
    /// 64 inputs (`managed_embedding_inputs` ordinal 0..63 for statements).
    fn checked_up_to(
        account_id: Uuid,
        graph_id: Uuid,
        job_id: Uuid,
        attributed_user_id: Uuid,
        inputs: Vec<ReceiptInput>,
        max: usize,
    ) -> Result<Self, EmbeddingError> {
        if !(1..=max).contains(&inputs.len()) {
            return Err(EmbeddingError::InvalidBatch);
        }
        let mut ids = HashSet::new();
        let mut bytes = 0i64;
        for input in &inputs {
            if input.text.is_empty() || input.content_revision <= 0 || !ids.insert(input.atom_id) {
                return Err(EmbeddingError::InvalidBatch);
            }
            bytes = bytes
                .checked_add(
                    i64::try_from(input.text.len()).map_err(|_| EmbeddingError::InvalidBatch)?,
                )
                .ok_or(EmbeddingError::InvalidBatch)?;
        }
        Ok(Self {
            account_id,
            graph_id,
            job_id,
            attributed_user_id,
            inputs,
        })
    }
}

fn input_micro(tokens: u64, rate: f64) -> Option<i64> {
    if !rate.is_finite() || rate < 0.0 {
        return None;
    }
    let decimal = rate.to_string();
    let (whole, fraction) = decimal.split_once('.').unwrap_or((&decimal, ""));
    let scale = 10u128.checked_pow(u32::try_from(fraction.len()).ok()?)?;
    let numerator = whole
        .parse::<u128>()
        .ok()?
        .checked_mul(scale)?
        .checked_add(if fraction.is_empty() {
            0
        } else {
            fraction.parse::<u128>().ok()?
        })?;
    let cost = u128::from(tokens).checked_mul(numerator)?;
    let rounded = cost
        .checked_div(scale)?
        .checked_add(u128::from(cost % scale != 0))?;
    i64::try_from(rounded).ok()
}

fn embedding_worst_case(
    batch: &ReceiptBatch,
    sheet: &PriceSheet,
    as_of: DateTime<Utc>,
) -> Result<WorstCase, RefuseCode> {
    let rate = sheet.rate("openrouter", MODEL, as_of);
    if !rate.matched {
        return Err(RefuseCode::UnpricedModel);
    }
    let bytes = batch
        .inputs
        .iter()
        .try_fold(0u64, |total, input| {
            total.checked_add(u64::try_from(input.text.len()).ok()?)
        })
        .ok_or(RefuseCode::CallCostCap)?;
    let micro = input_micro(bytes, rate.input_per_million)
        .filter(|_| bytes <= i64::MAX as u64)
        .ok_or(RefuseCode::CallCostCap)?
        .max(1);
    Ok(WorstCase {
        per_attempt: MicroUsd(micro),
        input_bound_tokens: bytes,
        max_tokens: 0,
        terms: SendTerms {
            models: vec![MODEL.into()],
            max_price: Some(MaxPrice {
                prompt: rate.input_per_million,
                completion: 0.0,
            }),
        },
    })
}

#[cfg(test)]
async fn reserve_inputs_tx(
    tx: &mut Transaction<'_, Postgres>,
    batch: &ReceiptBatch,
    logical_call: Uuid,
    worst: &WorstCase,
    caps: &Caps,
) -> Result<Uuid, EmbeddingError> {
    super::gate::attempt_cap_check(worst, caps.per_attempt).map_err(EmbeddingError::Refused)?;
    let graph: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM graphs WHERE id=$1 AND account_id=$2 FOR SHARE")
            .bind(batch.graph_id)
            .bind(batch.account_id)
            .fetch_optional(&mut **tx)
            .await?;
    if graph.is_none() {
        return Err(EmbeddingError::Stale);
    }
    let mut ordered: Vec<_> = batch.inputs.iter().collect();
    ordered.sort_unstable_by_key(|input| input.atom_id);
    for input in ordered {
        let revision: Option<i64> = sqlx::query_scalar(
            "SELECT content_revision FROM atoms WHERE atom_id=$1 AND graph_id=$2 FOR UPDATE",
        )
        .bind(input.atom_id)
        .bind(batch.graph_id)
        .fetch_optional(&mut **tx)
        .await?;
        if revision != Some(input.content_revision) {
            return Err(EmbeddingError::Stale);
        }
    }
    let period: chrono::NaiveDate = sqlx::query_scalar("SELECT (now() AT TIME ZONE 'UTC')::date")
        .fetch_one(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO managed_budget_periods(account_id,purpose,period_start,limit_micro_usd) VALUES($1,'cloud_embedding',$2,$3) ON CONFLICT(account_id,purpose,period_start) DO NOTHING")
        .bind(batch.account_id).bind(period).bind(caps.daily(ManagedPurpose::CloudEmbedding).get()).execute(&mut **tx).await?;
    let reserved: Option<i32> = sqlx::query_scalar("UPDATE managed_budget_periods SET reserved_micro_usd=reserved_micro_usd+$3,attempts=attempts+1 WHERE account_id=$1 AND purpose='cloud_embedding' AND period_start=$2 AND reserved_micro_usd+charged_micro_usd+$3<=limit_micro_usd RETURNING 1")
        .bind(batch.account_id).bind(period).bind(worst.per_attempt.get()).fetch_optional(&mut **tx).await?;
    if reserved.is_none() {
        return Err(EmbeddingError::DailyBudget);
    }
    let receipt = sqlx::query_scalar("INSERT INTO managed_attempt_receipts(account_id,purpose,period_start,graph_id,job_id,attributed_user_id,logical_call,attempt_no,candidate_models,input_bound_tokens,max_tokens,reserved_micro_usd) VALUES($1,'cloud_embedding',$2,$3,$4,$5,$6,1,$7,$8,0,$9) RETURNING receipt_id")
        .bind(batch.account_id).bind(period).bind(batch.graph_id).bind(batch.job_id).bind(batch.attributed_user_id).bind(logical_call).bind(&worst.terms.models).bind(worst.input_bound_tokens as i64).bind(worst.per_attempt.get()).fetch_one(&mut **tx).await?;
    for (ordinal, input) in batch.inputs.iter().enumerate() {
        sqlx::query("INSERT INTO managed_embedding_inputs(receipt_id,account_id,graph_id,atom_id,ordinal,content_revision,content_sha256,input_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(receipt).bind(batch.account_id).bind(batch.graph_id).bind(input.atom_id).bind(ordinal as i16).bind(input.content_revision).bind(Sha256::digest(input.text.as_bytes()).to_vec()).bind(input.text.len() as i64).execute(&mut **tx).await?;
    }
    Ok(receipt)
}

#[cfg(test)]
struct EmbeddingGate<'a> {
    plane: &'a JobScope<'a>,
    batch: ReceiptBatch,
    logical_call: Uuid,
    worst: WorstCase,
    caps: Caps,
    sheet: PriceSheet,
    reserved_once: AtomicBool,
}

#[cfg(test)]
impl EmbeddingGate<'_> {
    async fn begin(&self) -> Result<Transaction<'_, Postgres>, crate::store::StoreError> {
        match self.plane {
            JobScope::Consumer { pool, account_id } => {
                begin_account_scoped(pool, *account_id).await
            }
            JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
        }
    }

    async fn reserve_attempt(&self, attempt_no: u32) -> Result<Reservation, Deny> {
        if attempt_no != 1 || self.reserved_once.swap(true, Ordering::AcqRel) {
            return Err(Deny::Unavailable);
        }
        if self.caps.disabled {
            return Err(Deny::Hold(HoldCode::ProcessingDisabled));
        }
        let mut tx = self.begin().await.map_err(|_| Deny::Unavailable)?;
        let receipt = reserve_inputs_tx(
            &mut tx,
            &self.batch,
            self.logical_call,
            &self.worst,
            &self.caps,
        )
        .await
        .map_err(|error| match error {
            EmbeddingError::DailyBudget => Deny::Hold(HoldCode::DailyBudgetExhausted),
            EmbeddingError::Refused(code) => Deny::Refuse(code),
            _ => Deny::Unavailable,
        })?;
        tx.commit().await.map_err(|_| Deny::Unavailable)?;
        Ok(Reservation {
            receipt_id: Some(receipt),
        })
    }

    fn billed_micro(&self, outcome: &SendOutcome) -> Option<i64> {
        billed_micro(&self.worst, outcome)
    }

    async fn settle_attempt(&self, sent: Sent, outcome: &SendOutcome) {
        let Some(receipt) = sent.receipt_id() else {
            return;
        };
        let cost = self.billed_micro(outcome);
        let (state, cost, model, upstream, generation) = match outcome {
            SendOutcome::Billed {
                served_model,
                upstream,
                generation_id,
                ..
            } if cost.is_some() => (
                "billed",
                cost,
                Some(served_model.as_str()),
                upstream.as_deref(),
                generation_id.as_deref(),
            ),
            SendOutcome::NotGenerated => ("not_generated", Some(0), None, None, None),
            SendOutcome::Ambiguous { generation_id } => {
                ("ambiguous", None, None, None, generation_id.as_deref())
            }
            SendOutcome::Billed { generation_id, .. } => {
                ("ambiguous", None, None, None, generation_id.as_deref())
            }
        };
        // Provider-reported strings, bound as `text` (`pg_text`): a NUL in
        // one would fail the settle and strand the reservation.
        use coralswarm_core::pg_text::sanitize_opt;
        let (model, upstream, generation) =
            (sanitize_opt(model), sanitize_opt(upstream), sanitize_opt(generation));
        for backoff in [Some(200), Some(800), None] {
            let result = async {
                let mut tx = self.begin().await?;
                sqlx::query_scalar::<_, bool>("SELECT managed_settle($1,$2,$3,$4,$5,$6)")
                    .bind(receipt)
                    .bind(state)
                    .bind(cost)
                    .bind(model.as_deref())
                    .bind(upstream.as_deref())
                    .bind(generation.as_deref())
                    .fetch_one(&mut *tx)
                    .await?;
                tx.commit().await?;
                Ok::<_, crate::store::StoreError>(())
            }
            .await;
            if result.is_ok() {
                return;
            }
            if let Some(ms) = backoff {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            } else {
                tracing::warn!(alert = true, %receipt, "embedding settle failed; reservation remains for reaping");
            }
        }
    }
}

#[cfg(test)]
impl SendGate for EmbeddingGate<'_> {
    fn terms(&self) -> Option<&SendTerms> {
        Some(&self.worst.terms)
    }
    fn reserve<'b>(&'b self, attempt_no: u32) -> GateFuture<'b, Result<Reservation, Deny>> {
        Box::pin(self.reserve_attempt(attempt_no))
    }
    fn settle<'b>(&'b self, sent: Sent, outcome: &'b SendOutcome) -> GateFuture<'b, ()> {
        Box::pin(self.settle_attempt(sent, outcome))
    }
}

#[cfg(test)]
mod pg_tests;

pub(super) fn billed_micro(worst: &WorstCase, outcome: &SendOutcome) -> Option<i64> {
    let SendOutcome::Billed {
        reported_cost_usd,
        input_tokens,
        served_model,
        ..
    } = outcome
    else {
        return None;
    };
    if let Some(cost) = reported_cost_usd {
        let micro = (*cost * 1_000_000.0).ceil();
        return (cost.is_finite() && *cost >= 0.0 && micro < i64::MAX as f64)
            .then_some(micro as i64);
    }
    // OpenRouter reports the bare upstream id, without the `openai/` prefix.
    if served_model != MODEL && Some(served_model.as_str()) != MODEL.strip_prefix("openai/") {
        return None;
    }
    worst
        .terms
        .max_price
        .and_then(|price| input_micro(*input_tokens, price.prompt))
}
