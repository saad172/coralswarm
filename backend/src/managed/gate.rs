//! The managed send gate: reserve before every provider HTTP attempt, settle
//! after it.
//!
//! It is the front door for every managed purpose (extraction, cloud
//! embedding, enrichment and digests, interactive Ask). [`door`] resolves
//! the account's liability plane once, before any write, through
//! `managed::plane`, and the gate carries the resulting [`PlanePermit`]:
//! every attempt's locked fence re-confirms it, and the funded reservation
//! takes its plane label from it.
//!

mod funded;
pub(crate) use funded::recover_logical_call;

#[path = "gate/pg_tests.rs"]
#[cfg(test)]
mod pg_tests;

use chrono::Utc;
use coralswarm_core::send_gate::{
    Deny, GateFuture, HoldCode, RefuseCode, Reservation, SendGate, SendOutcome, SendTerms, Sent,
    TransportPolicy,
};
use sqlx::{Postgres, Transaction};
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

use super::{worst_case, Caps, ManagedCall, ManagedPurpose, MicroUsd, PlanePermit, Subject, WorstCase};
use crate::billing::rates::PriceSheet;
use crate::llm::CompletionRequest;
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account};
use crate::workers::embed::JobScope;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

const SETTLE_BACKOFF_MS: [Option<u64>; 3] = [Some(200), Some(800), None];

pub(crate) struct ManagedGate<'a> {
    call: ManagedCall<'a>,
    /// Minted by [`door`] when the gate opened. Every fence and the funded
    /// reservation take it, so no attempt reaches either without the
    /// resolver.
    permit: PlanePermit,
    purpose: ManagedPurpose,
    logical_call: Uuid,
    worst: WorstCase,
    caps: Caps,
    sheet: PriceSheet,
    pricing_as_of: chrono::DateTime<Utc>,
    request_hash: String,
    embedding_reserved: AtomicBool,
    /// Resolved at open from the purpose, so a call's bounds never change
    /// between its attempts.
    transport: TransportPolicy,
}

pub(crate) fn attempt_cap_check(
    worst: &WorstCase,
    per_attempt: MicroUsd,
) -> Result<(), RefuseCode> {
    if worst.per_attempt > per_attempt {
        Err(RefuseCode::CallCostCap)
    } else {
        Ok(())
    }
}

async fn begin_plane<'a>(
    plane: &JobScope<'a>,
) -> Result<Transaction<'a, Postgres>, crate::store::StoreError> {
    let mut tx = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        match plane {
            JobScope::Consumer { pool, account_id } => {
                begin_account_scoped(pool, *account_id).await
            }
            JobScope::Org(pool) => begin_tenant_system_scoped_without_account(pool).await,
        }
    })
    .await
    .map_err(|e| crate::store::StoreError::Db(Box::new(e)))??;
    sqlx::query(
        "SELECT set_config('lock_timeout','2s',true),set_config('statement_timeout','5s',true)",
    )
    .execute(&mut *tx)
    .await?;
    Ok(tx)
}

/// The front door's plane check, once per gate and before any write: the
/// account must resolve (`managed::plane`) to the plane this call runs on.
/// A personal org resolves to the consumer plane (COR-114).
async fn door(call: &ManagedCall<'_>) -> Result<PlanePermit, Deny> {
    let mut tx = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        begin_account_scoped(call.control, call.account_id),
    )
    .await
    .map_err(|_| Deny::Unavailable)?
    .map_err(|_| Deny::Unavailable)?;
    let permit = PlanePermit::admit(&mut tx, call.account_id, call.tenant).await?;
    // Read-only: nothing to commit. Each attempt's locked fence re-confirms.
    tx.rollback().await.map_err(|_| Deny::Unavailable)?;
    Ok(permit)
}

impl<'a> ManagedGate<'a> {
    /// No send. Resolves the plane ([`door`]), loads the price sheet and
    /// caps, computes the worst case and enforces the per-attempt cap. The
    /// only write is a cohort window's hold when the door refuses it.
    pub(crate) async fn open(
        mut call: ManagedCall<'a>,
        req: &CompletionRequest,
    ) -> Result<Self, Deny> {
        if matches!(call.subject, Subject::EmbeddingBatch { .. }) {
            return Err(Deny::Unavailable);
        }
        if let Subject::Enrichment { work } = &call.subject {
            // The activation switch, checked before any write; the locked
            // fence re-checks it before every reservation.
            if !super::enrichment::enabled() {
                return Err(Deny::Hold(HoldCode::ProcessingDisabled));
            }
            work.check_call(call.account_id, call.principal)?;
        }
        if let Subject::InteractiveAsk { work } = &call.subject {
            work.check_call(call.account_id, call.principal)?;
        }
        let permit = match door(&call).await {
            Ok(permit) => permit,
            Err(Deny::Hold(code)) => return Err(record_hold(&call, code).await),
            Err(deny) => return Err(deny),
        };
        let purpose = purpose_of(&call.subject);
        let caps = Caps::from_env();
        let sheet = PriceSheet::load(call.control).await.map_err(|e| {
            tracing::warn!(error = %e, "managed gate: price sheet unavailable");
            Deny::Unavailable
        })?;
        let pricing_as_of = Utc::now();
        let worst = worst_case(req, call.fallback_model.as_deref(), &sheet, caps.headroom, pricing_as_of)
            .map_err(Deny::Refuse)?;
        attempt_cap_check(&worst, caps.per_attempt).map_err(Deny::Refuse)?;
        use sha2::{Digest, Sha256};
        let mut material = serde_json::json!({
            "request": req,
            "models": worst.terms.models,
            "ceiling_prompt": worst.ceiling().prompt,
            "ceiling_completion": worst.ceiling().completion,
            "bound_micro_usd": worst.per_attempt.get(),
        });
        if let Subject::Enrichment { work } = &call.subject {
            // Binds every input (kind, id, revision, sha256) and the payer.
            material["enrichment"] = work.binding(call.tenant, call.principal);
        }
        if let Subject::InteractiveAsk { work } = &call.subject {
            material["interactive_ask"] = work.binding(call.tenant);
        }
        let request_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&material).map_err(|_| Deny::Unavailable)?,
        ));
        let logical_call = match &call.subject {
            Subject::StandingWindow { work } => work.logical_call,
            Subject::Window { .. } => Uuid::new_v4(),
            Subject::Enrichment { work } => work.logical_call(),
            Subject::InteractiveAsk { work } => work.logical_call(),
            Subject::EmbeddingBatch { .. } => return Err(Deny::Unavailable),
        };
        if matches!(call.subject, Subject::StandingWindow { .. }) {
            if let Some(payer) = crate::billing::managed_reservation::existing_payer(
                call.control,
                &permit.label(),
                call.account_id,
                logical_call,
            )
            .await
            .map_err(|e| {
                tracing::warn!(error=%e,"managed frozen payer unavailable");
                Deny::Unavailable
            })? {
                call.principal = payer;
            }
        }
        Ok(Self {
            call,
            permit,
            purpose,
            logical_call,
            worst,
            caps,
            sheet,
            pricing_as_of,
            request_hash,
            embedding_reserved: AtomicBool::new(false),
            transport: super::transport::policy_for(
                purpose,
                false,
                std::time::Instant::now(),
                super::transport::current_call_deadline(),
                std::time::Duration::from_secs(caps.reap_after_secs),
                &super::transport::TransportEnv::from_env(),
            ),
        })
    }

    /// A second, independent logical call charged to the same standing
    /// window: same purpose, unit, per-version cap and fresh fence, its own
    /// `logical_call`. Extraction v2 uses it for Jev's binding question, so
    /// the window's extraction call keeps its pinned logical call and attempt
    /// numbers. Only a standing window: its holds are not written by the
    /// gate, so a held aside call never pauses the window it belongs to.
    pub(crate) async fn open_aside(
        call: ManagedCall<'a>,
        req: &CompletionRequest,
    ) -> Result<Self, Deny> {
        if !matches!(call.subject, Subject::StandingWindow { .. }) {
            return Err(Deny::Unavailable);
        }
        let mut gate = Self::open(call, req).await?;
        gate.logical_call = Uuid::new_v4();
        gate.transport = super::transport::policy_for(
            gate.purpose,
            true,
            std::time::Instant::now(),
            super::transport::current_call_deadline(),
            std::time::Duration::from_secs(gate.caps.reap_after_secs),
            &super::transport::TransportEnv::from_env(),
        );
        Ok(gate)
    }

    pub(in crate::managed) async fn embedding(call: ManagedCall<'a>) -> Result<Self,Deny> {
        let Subject::EmbeddingBatch {batch}=call.subject else {return Err(Deny::Unavailable);};
        if call.account_id != batch.account_id || call.principal != batch.principal {
            return Err(Deny::Unavailable);
        }
        // The caller records an embedding batch's hold from the returned code.
        let permit = door(&call).await?;
        let caps = Caps::from_env();
        let sheet = PriceSheet::load(call.control)
            .await
            .map_err(|_| Deny::Unavailable)?;
        let pricing_as_of = Utc::now();
        let worst = batch
            .worst_case(&sheet, pricing_as_of)
            .map_err(Deny::Refuse)?;
        attempt_cap_check(&worst, caps.per_attempt).map_err(Deny::Refuse)?;
        let request_hash = batch.request_hash(call.tenant, &worst)?;
        Ok(Self {
            call,
            permit,
            purpose: ManagedPurpose::CloudEmbedding,
            logical_call: Uuid::new_v4(),
            worst,
            caps,
            sheet,
            pricing_as_of,
            request_hash,
            embedding_reserved: AtomicBool::new(false),
            transport: TransportPolicy::default(),
        })
    }

    pub(in crate::managed) fn embedding_binding(&self) -> (Uuid, &str) {
        (self.logical_call, &self.request_hash)
    }

    fn graph_job(&self) -> (Uuid, Option<Uuid>) {
        match self.call.subject {
            Subject::EmbeddingBatch { batch } => (batch.graph_id, Some(batch.job_id)),
            Subject::StandingWindow { work } => (work.graph_id, Some(work.job_id)),
            Subject::Window { graph, job, .. } => (graph, Some(job)),
            Subject::Enrichment { work } => (work.graph_id(), work.job_id()),
            Subject::InteractiveAsk { work } => (work.graph_id(), None),
        }
    }

    fn window(&self) -> Option<(Uuid, Uuid, &'a str, Uuid)> {
        window_of(&self.call.subject)
    }

    async fn hold(&self, code: HoldCode) -> Deny {
        record_hold(&self.call, code).await
    }

    async fn reserve_attempt(&self, attempt_no: u32) -> Result<Reservation, Deny> {
        if matches!(self.call.subject, Subject::EmbeddingBatch { .. })
            && (attempt_no != 1 || self.embedding_reserved.swap(true, Ordering::AcqRel))
        {
            return Err(Deny::Unavailable);
        }
        if matches!(self.call.subject, Subject::Enrichment { .. })
            && !(1..=super::enrichment::MAX_ATTEMPTS).contains(&attempt_no)
        {
            return Err(Deny::Unavailable);
        }
        if matches!(self.call.subject, Subject::InteractiveAsk { .. })
            && !(1..=super::interactive_ask::MAX_ATTEMPTS).contains(&attempt_no)
        {
            return Err(Deny::Unavailable);
        }
        funded::reserve(self, attempt_no).await
    }

    /// The charge for a billed attempt: the provider's figure, else its tokens
    /// at the served model's card rate, else at the reserved ceiling.
    fn billed_micro(&self, outcome: &SendOutcome) -> i64 {
        let SendOutcome::Billed {
            reported_cost_usd,
            input_tokens,
            output_tokens,
            served_model,
            ..
        } = outcome
        else {
            return 0;
        };
        if let Some(cost) = reported_cost_usd {
            return MicroUsd::from_usd_ceil(*cost).get();
        }
        let rate = self.sheet.rate("openrouter", served_model, self.pricing_as_of);
        let (prompt, completion) = if rate.matched {
            (rate.input_per_million, rate.output_per_million)
        } else {
            let ceiling = self.worst.ceiling();
            (ceiling.prompt, ceiling.completion)
        };
        let micro = *input_tokens as f64 * prompt + *output_tokens as f64 * completion;
        MicroUsd::from_usd_ceil(micro / 1_000_000.0).get()
    }

    async fn settle_attempt(&self, sent: Sent, outcome: &SendOutcome) {
        let Some(receipt) = sent.receipt_id() else {
            return;
        };
        let (state, cost, model, upstream, generation): (
            &str,
            Option<i64>,
            Option<&str>,
            Option<&str>,
            Option<&str>,
        ) = match outcome {
            SendOutcome::Billed { generation_id, .. }
                if matches!(self.call.subject, Subject::EmbeddingBatch { .. })
                    && super::embedding::billed_micro(&self.worst, outcome).is_none() =>
            {
                ("ambiguous", None, None, None, generation_id.as_deref())
            }
            SendOutcome::Billed {
                served_model,
                upstream,
                generation_id,
                ..
            } => (
                "billed",
                if matches!(self.call.subject, Subject::EmbeddingBatch { .. }) {
                    super::embedding::billed_micro(&self.worst, outcome)
                } else {
                    Some(self.billed_micro(outcome))
                },
                Some(served_model.as_str()),
                upstream.as_deref(),
                generation_id.as_deref(),
            ),
            SendOutcome::NotGenerated => ("not_generated", Some(0), None, None, None),
            SendOutcome::Ambiguous { generation_id } => {
                ("ambiguous", None, None, None, generation_id.as_deref())
            }
        };
        if cost.is_some_and(|c| c > self.worst.per_attempt.get()) {
            tracing::warn!(
                alert = true,
                %receipt,
                charged = cost.unwrap_or_default(),
                reserved = self.worst.per_attempt.get(),
                "managed gate: attempt billed above its reservation (overrun); charged in full"
            );
        }
        // Provider-reported strings, bound as `text` (`pg_text`): a NUL in
        // one would fail the settle and leave the reservation to the reaper.
        use coralswarm_core::pg_text::sanitize_opt;
        let (model, upstream, generation) =
            (sanitize_opt(model), sanitize_opt(upstream), sanitize_opt(generation));
        for (attempt, backoff_ms) in SETTLE_BACKOFF_MS.iter().enumerate() {
            let settled = async {
                let mut tx = begin_plane(self.call.plane).await?;
                sqlx::query_scalar::<_, bool>("SELECT managed_settle_v1($1,$2,$3,$4,$5,$6)")
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
            let Err(e) = settled else {
                if let Err(e) = funded::reconcile(self, receipt).await {
                    tracing::warn!(error = %e, %receipt, "managed credit settlement pending billing reconciliation");
                }
                return;
            };
            match backoff_ms {
                Some(ms) => {
                    tracing::warn!(error = %e, %receipt, attempt = attempt + 1, "managed gate: settle failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(*ms)).await;
                }
                None => tracing::warn!(
                    alert = true,
                    error = %e,
                    %receipt,
                    "managed gate: settle failed after retries; the reaper will charge the reservation"
                ),
            }
        }
    }
}

/// A cohort or standing window's (graph, version, profile, job).
fn window_of<'a>(subject: &Subject<'a>) -> Option<(Uuid, Uuid, &'a str, Uuid)> {
    match subject {
        Subject::StandingWindow { work } => Some((
            work.graph_id,
            work.version_id,
            work.profile.as_str(),
            work.job_id,
        )),
        Subject::Window {
            graph,
            version,
            profile,
            job,
        } => Some((*graph, *version, *profile, *job)),
        Subject::EmbeddingBatch { .. } => None,
        Subject::Enrichment { .. } => None,
        Subject::InteractiveAsk { .. } => None,
    }
}

/// Records a hold where the subject keeps it, and returns it as the denial.
async fn record_hold(call: &ManagedCall<'_>, code: HoldCode) -> Deny {
    if let Subject::EmbeddingBatch { batch } = call.subject {
        return match batch.hold(call.plane, code.code()).await {
            Ok(()) => Deny::Hold(code),
            Err(error) => error,
        };
    }
    if matches!(call.subject, Subject::StandingWindow { .. }) {
        return Deny::Hold(code);
    }
    if matches!(call.subject, Subject::Enrichment { .. }) {
        // No unit or version to stamp: the worker records the hold on
        // its own job (or sweep cursor) from the returned code.
        return Deny::Hold(code);
    }
    if matches!(call.subject, Subject::InteractiveAsk { .. }) {
        // Nothing to pause: the Ask phase fails open from the code.
        return Deny::Hold(code);
    }
    let Some((graph, version, profile, _)) = window_of(&call.subject) else {
        return Deny::Unavailable;
    };
    let written = async {
        let mut tx = begin_plane(call.plane).await?;
        crate::corpus_enrichment::hold_window_tx(&mut tx, graph, version, profile, code.code())
            .await?;
        tx.commit().await?;
        Ok::<_, crate::store::StoreError>(())
    }
    .await;
    match written {
        Ok(()) => Deny::Hold(code),
        Err(e) => {
            tracing::warn!(error = %e, reason = code.code(), "managed gate: failed to record the hold");
            Deny::Unavailable
        }
    }
}

/// The ledger purpose a subject is charged to.
fn purpose_of(subject: &Subject<'_>) -> ManagedPurpose {
    match subject {
        Subject::EmbeddingBatch { .. } => ManagedPurpose::CloudEmbedding,
        Subject::StandingWindow { .. } | Subject::Window { .. } => ManagedPurpose::SourceExtraction,
        Subject::Enrichment { .. } => ManagedPurpose::Enrichment,
        Subject::InteractiveAsk { .. } => ManagedPurpose::InteractiveAsk,
    }
}

impl SendGate for ManagedGate<'_> {
    fn terms(&self) -> Option<&SendTerms> {
        Some(&self.worst.terms)
    }

    fn transport(&self) -> TransportPolicy {
        self.transport.clone()
    }

    fn reserve<'b>(&'b self, attempt_no: u32) -> GateFuture<'b, Result<Reservation, Deny>> {
        Box::pin(self.reserve_attempt(attempt_no))
    }

    fn settle<'b>(&'b self, sent: Sent, outcome: &'b SendOutcome) -> GateFuture<'b, ()> {
        Box::pin(self.settle_attempt(sent, outcome))
    }
}

/// Settles receipts left `reserved` past the per-attempt deadline as
/// ambiguous, at the full reservation. One call covers the whole plane.
pub(crate) async fn reap_tick(t: &WorkerTenant) -> Result<Tick, String> {
    let after = Caps::from_env().reap_after_secs as f64;
    let reaped: i32 = sqlx::query_scalar(
        "SELECT managed_reap_stale(make_interval(secs => $1)) \
              + managed_reap_stale_v1(make_interval(secs => $1))",
    )
    .bind(after)
    .fetch_one(&t.pool)
    .await
    .map_err(|e| format!("managed receipt reaper: {e}"))?;
    if reaped > 0 {
        tracing::warn!(
            reaped,
            tenant = ?t.tenant,
            "managed receipt reaper: charged stale reservations as ambiguous"
        );
        Ok(Tick::worked(reaped as usize))
    } else {
        Ok(Tick::idle())
    }
}
