//! `managed_enrichment`: the one shared managed purpose for graph-level
//! consumers (entity resolution, journal, atom titles, digests, meeting
//! summaries), under one daily cap per account
//! (`MANAGED_DAILY_CAP_USD_ENRICHMENT`, default $2).
//!
//! Workers move onto the purpose one at a time. `EntityResolution`
//! (`workers::entity_resolution`) moved first; reef digests
//! (`workers::digest`), coral titles (`workers::atom_title_backfill`) and the
//! journal sweep (`workers::journal_sweep`, no `jobs` row) followed. Each
//! consumer also has its own kill switch ([`consumer_enabled`]) and a daily
//! per-graph call bound counted from its marker input ([`calls_today`]).
//!
//! How a worker uses it, in order:
//! 1. At claim: skip unless [`claim_allowed`] (the `ENRICHMENT_PROCESSING_ENABLED`
//!    switch, off by default, and the global `MANAGED_PROCESSING_DISABLED`
//!    kill switch). After claiming a job, call [`recover_job`] once so an
//!    earlier crashed attempt's receipts are settled.
//! 2. Read the inputs and build an [`EnrichmentWork`]: every input carries
//!    `(kind, id, content_revision, content_sha256)`.
//! 3. [`select_principal`] picks the member the call is charged to: the
//!    work's explicit attributed user when it has one (journal), else the
//!    graph's executor (personal/member ocean owner, else the earliest
//!    owner/admin who can reach the graph), after the account policy.
//! 4. Call `llm::usage::metered_complete_managed` with
//!    `Subject::Enrichment { work: &work }`. The gate refuses unless the
//!    switch is on, binds every input into the request hash, and for each
//!    attempt re-checks the switch, policy, principal and job inside the
//!    locked fence, reserves the shared daily budget and writes every input to
//!    `managed_enrichment_inputs` before the send is armed.
//! 5. Publish in one transaction: re-read the current inputs, call
//!    [`verify_inputs_locked`], and persist outputs only on
//!    [`InputCheck::Fresh`], then [`Verified::mark_written`] in the same
//!    transaction. On [`InputCheck::Stale`] commit without outputs.
//! 6. When an input is deleted, [`invalidate_inputs`] is the one query that
//!    fences in-flight calls and names the receipts whose outputs were built
//!    from it.

// Deletion invalidation runs in SQL (`managed_enrichment_invalidate_atoms`,
// tenant 0171 / control 0338); `invalidate_inputs` stays the Rust twin for
// tests and future non-atom inputs.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeSet;

use coralswarm_core::send_gate::{Deny, HoldCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{embedding, Caps, ManagedPurpose, PlanePermit};
use crate::tenancy::TenantId;
use crate::workers::embed::JobScope;

/// Inputs one call may bind. A digest reads about 30 inputs and a journal
/// hour about 150; the table's ordinal CHECK is the same bound.
pub(crate) const MAX_INPUTS: usize = 1024;

/// Provider attempts one logical call may reserve (the receipt CHECK allows
/// 1..=8).
pub(crate) const MAX_ATTEMPTS: u32 = 8;

const PURPOSE: ManagedPurpose = ManagedPurpose::Enrichment;

fn switch_on(value: Option<&str>) -> bool {
    value == Some("true")
}

/// The activation switch. Off unless `ENRICHMENT_PROCESSING_ENABLED` is
/// exactly `true`; read per call so rollback is an env change.
pub(crate) fn enabled() -> bool {
    switch_on(std::env::var("ENRICHMENT_PROCESSING_ENABLED").ok().as_deref())
}

/// The claim-time check: the switch is on and the global managed kill switch
/// is off. The gate's locked fence checks both again before every reserve.
pub(crate) fn claim_allowed() -> bool {
    enabled() && !Caps::from_env().disabled
}

fn consumer_switch(value: Option<&str>) -> bool {
    !matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("false" | "0" | "off" | "no")
    )
}

/// One consumer's own kill switch (`JOURNAL_SWEEP_ENABLED`,
/// `ATOM_TITLES_ENABLED`, `DIGESTS_ENABLED`): on unless set to `false`, `0`,
/// `off` or `no`. It only narrows [`claim_allowed`]; the shared
/// `ENRICHMENT_PROCESSING_ENABLED` switch still has to be on. Read per call so
/// rollback is an env change.
pub(crate) fn consumer_enabled(var: &str) -> bool {
    consumer_switch(std::env::var(var).ok().as_deref())
}

/// [`claim_allowed`] and the consumer's own switch.
pub(crate) fn consumer_claim_allowed(var: &str) -> bool {
    claim_allowed() && consumer_enabled(var)
}

/// A non-negative per-graph daily bound from `var`, else `default`.
pub(crate) fn daily_bound(var: &str, default: i64) -> i64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|n| *n >= 0)
        .unwrap_or(default)
}

/// Logical calls opened on `graph` (every graph of the account when `None`)
/// this UTC day whose receipts bound a `marker` input. Each consumer binds one marker input kind per call
/// (`journal_prompt`, `title_batch`, `digest_node`; entity resolution's
/// `er_facts`), so this is that consumer's spend count for the day. Run it in
/// the worker's plane scope (account scope on the consumer plane, tenant
/// system scope on an org tenant).
pub(crate) async fn calls_today(
    conn: &mut PgConnection,
    account: Uuid,
    graph: Option<Uuid>,
    marker: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(DISTINCT r.logical_call) FROM managed_attempt_receipts r \
          WHERE r.account_id=$1 AND ($2::uuid IS NULL OR r.graph_id=$2) AND r.purpose='managed_enrichment' \
            AND r.period_start=(clock_timestamp() AT TIME ZONE 'UTC')::date \
            AND EXISTS(SELECT 1 FROM managed_enrichment_inputs i WHERE i.receipt_id=r.receipt_id AND i.input_kind=$3)",
    )
    .bind(account)
    .bind(graph)
    .bind(marker)
    .fetch_one(conn)
    .await
}

/// A non-negative per-account daily dollar bound from `var` (USD, e.g.
/// `1.40`), else `default_usd`, in micro-USD.
pub(crate) fn daily_usd_bound(var: &str, default_usd: f64) -> i64 {
    let usd = std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|n| n.is_finite() && *n >= 0.0)
        .unwrap_or(default_usd);
    (usd * 1_000_000.0).round() as i64
}

/// A consumer's daily dollar share of the account's shared enrichment cap
/// (`MANAGED_DAILY_CAP_USD_ENRICHMENT`), in micro-USD: the fraction in
/// `var` (0.0..=1.0, e.g. `0.5`), else `default_share`, times the cap. A
/// fraction, not a dollar figure, so the share scales when the cap is
/// raised and the consumers without one (journal, coral titles, the
/// supersession judge) keep the rest. The shared cap still holds whatever
/// the shares add up to.
pub(crate) fn daily_share_micro_usd(var: &str, default_share: f64) -> i64 {
    let share = std::env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|n| n.is_finite() && (0.0..=1.0).contains(n))
        .unwrap_or(default_share);
    (Caps::from_env().daily_enrichment.get() as f64 * share).floor() as i64
}

/// Micro-USD this UTC day on `graph` (every graph of the account when
/// `None`) across the attempts whose receipts bound a `marker` input: the
/// settled charge, or the reservation while an attempt is still open. The
/// dollar twin of [`calls_today`], for a consumer whose share of the shared
/// daily cap is a dollar amount rather than a call count.
pub(crate) async fn spend_today_micro_usd(
    conn: &mut PgConnection,
    account: Uuid,
    graph: Option<Uuid>,
    marker: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COALESCE(sum(COALESCE(r.settled_micro_usd, r.reserved_micro_usd)),0)::bigint FROM managed_attempt_receipts r \
          WHERE r.account_id=$1 AND ($2::uuid IS NULL OR r.graph_id=$2) AND r.purpose='managed_enrichment' \
            AND r.period_start=(clock_timestamp() AT TIME ZONE 'UTC')::date \
            AND EXISTS(SELECT 1 FROM managed_enrichment_inputs i WHERE i.receipt_id=r.receipt_id AND i.input_kind=$3)",
    )
    .bind(account)
    .bind(graph)
    .bind(marker)
    .fetch_one(conn)
    .await
}

/// Seconds until the next UTC midnight (plus a minute of slack), for a pass
/// that stopped on a daily bound or the daily cap.
pub(crate) fn secs_until_next_utc_day(now: chrono::DateTime<chrono::Utc>) -> i64 {
    let tomorrow = (now + chrono::Duration::days(1)).date_naive().and_hms_opt(0, 1, 0).expect("valid time");
    (tomorrow.and_utc() - now).num_seconds().max(60)
}

fn unavailable(error: impl std::fmt::Display) -> Deny {
    tracing::warn!(%error, "managed enrichment unavailable");
    Deny::Unavailable
}

/// One thing a graph-level call reads. `content_sha256` is over the exact
/// bytes the prompt used, so publish can reject a changed input even when
/// its revision counter did not move.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct EnrichmentInput {
    pub kind: String,
    pub id: Uuid,
    pub content_revision: i64,
    pub content_sha256: [u8; 32],
}

impl EnrichmentInput {
    pub(crate) fn new(kind: &str, id: Uuid, content_revision: i64, content: &[u8]) -> Self {
        Self::with_digest(kind, id, content_revision, Sha256::digest(content).into())
    }

    pub(crate) fn with_digest(
        kind: &str,
        id: Uuid,
        content_revision: i64,
        content_sha256: [u8; 32],
    ) -> Self {
        Self {
            kind: kind.to_owned(),
            id,
            content_revision,
            content_sha256,
        }
    }

    fn key(&self) -> (&str, Uuid) {
        (self.kind.as_str(), self.id)
    }
}

/// `^[a-z][a-z0-9_]{0,47}$`, mirrored by the table CHECK.
fn valid_kind(kind: &str) -> bool {
    let bytes = kind.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 48
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

/// The queue row a call runs under, fenced by `(id, kind, attempts)` while
/// `running`. Sweeps that are not queue jobs (journal) pass `None` and hold
/// their own lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EnrichmentJob {
    pub id: Uuid,
    pub kind: String,
    pub attempts: i32,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum InvalidWork {
    #[error("an enrichment call binds 1..={MAX_INPUTS} inputs")]
    InputCount,
    #[error("input kind must match ^[a-z][a-z0-9_]{{0,47}}$")]
    InputKind,
    #[error("input revisions are non-negative")]
    Revision,
    #[error("an input is bound once per call")]
    Duplicate,
}

/// Everything one graph-level logical call binds before it is sent.
#[derive(Clone, Debug)]
pub(crate) struct EnrichmentWork {
    account_id: Uuid,
    graph_id: Uuid,
    job: Option<EnrichmentJob>,
    attributed_user: Option<Uuid>,
    logical_call: Uuid,
    inputs: Vec<EnrichmentInput>,
}

impl EnrichmentWork {
    pub(crate) fn new(
        account_id: Uuid,
        graph_id: Uuid,
        job: Option<EnrichmentJob>,
        attributed_user: Option<Uuid>,
        inputs: Vec<EnrichmentInput>,
    ) -> Result<Self, InvalidWork> {
        if !(1..=MAX_INPUTS).contains(&inputs.len()) {
            return Err(InvalidWork::InputCount);
        }
        let mut seen = BTreeSet::new();
        for input in &inputs {
            if !valid_kind(&input.kind) {
                return Err(InvalidWork::InputKind);
            }
            if input.content_revision < 0 {
                return Err(InvalidWork::Revision);
            }
            if !seen.insert(input.key()) {
                return Err(InvalidWork::Duplicate);
            }
        }
        Ok(Self {
            account_id,
            graph_id,
            job,
            attributed_user,
            logical_call: Uuid::new_v4(),
            inputs,
        })
    }

    pub(crate) fn graph_id(&self) -> Uuid {
        self.graph_id
    }
    pub(crate) fn account_id(&self) -> Uuid {
        self.account_id
    }
    pub(crate) fn job_id(&self) -> Option<Uuid> {
        self.job.as_ref().map(|job| job.id)
    }
    /// One logical call per work value: every attempt's receipt carries it,
    /// and publish finds the served receipt by it.
    pub(crate) fn logical_call(&self) -> Uuid {
        self.logical_call
    }
    pub(crate) fn inputs(&self) -> &[EnrichmentInput] {
        &self.inputs
    }

    /// The call must name this work's account and, when the work has an
    /// attributed user, charge exactly that user.
    pub(in crate::managed) fn check_call(&self, account: Uuid, principal: Uuid) -> Result<(), Deny> {
        if account != self.account_id || self.attributed_user.is_some_and(|user| user != principal) {
            return Err(Deny::Unavailable);
        }
        Ok(())
    }

    /// The request-hash material for this call: purpose, plane, account,
    /// graph, job, payer and every input in order.
    pub(in crate::managed) fn binding(&self, tenant: &TenantId, principal: Uuid) -> Value {
        let plane = match tenant {
            TenantId::Consumer => "consumer".to_owned(),
            TenantId::Org(org) => format!("org:{org}"),
        };
        let inputs: Vec<Value> = self
            .inputs
            .iter()
            .map(|input| {
                json!({
                    "kind": input.kind,
                    "id": input.id,
                    "revision": input.content_revision,
                    "sha256": hex::encode(input.content_sha256),
                })
            })
            .collect();
        json!({
            "purpose": PURPOSE.as_str(),
            "plane": plane,
            "account": self.account_id,
            "graph": self.graph_id,
            "job": self.job.as_ref().map(|job| json!({"id": job.id, "kind": job.kind, "attempts": job.attempts})),
            "principal": principal,
            "attributed_user": self.attributed_user,
            "logical_call": self.logical_call,
            "inputs": inputs,
        })
    }

    /// Written in the reserve transaction, after the receipt and before the
    /// arm, so no send exists without its full input set.
    pub(in crate::managed) async fn insert_inputs(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        receipt: Uuid,
    ) -> Result<(), Deny> {
        let ordinals: Vec<i16> = (0..self.inputs.len())
            .map(|i| i16::try_from(i).map_err(unavailable))
            .collect::<Result<_, _>>()?;
        let kinds: Vec<&str> = self.inputs.iter().map(|i| i.kind.as_str()).collect();
        let ids: Vec<Uuid> = self.inputs.iter().map(|i| i.id).collect();
        let revisions: Vec<i64> = self.inputs.iter().map(|i| i.content_revision).collect();
        let digests: Vec<Vec<u8>> = self.inputs.iter().map(|i| i.content_sha256.to_vec()).collect();
        let written = sqlx::query(
            "INSERT INTO managed_enrichment_inputs \
                (receipt_id,account_id,graph_id,ordinal,input_kind,input_id,content_revision,content_sha256) \
             SELECT $1,$2,$3,o,k,i,r,d FROM unnest($4::smallint[],$5::text[],$6::uuid[],$7::bigint[],$8::bytea[]) AS t(o,k,i,r,d)",
        )
        .bind(receipt)
        .bind(self.account_id)
        .bind(self.graph_id)
        .bind(&ordinals)
        .bind(&kinds)
        .bind(&ids)
        .bind(&revisions)
        .bind(&digests)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if written != self.inputs.len() as u64 {
            return Err(Deny::Unavailable);
        }
        Ok(())
    }

    /// The locked fence for one attempt: switch, kill switch, account policy,
    /// the same principal still selected, and the job still owned.
    pub(in crate::managed) async fn fence_locked(
        &self,
        content: &mut Transaction<'_, Postgres>,
        financial: Option<&mut Transaction<'_, Postgres>>,
        permit: &PlanePermit,
        principal: Uuid,
    ) -> Result<(), Deny> {
        if !enabled() {
            return Err(Deny::Hold(HoldCode::ProcessingDisabled));
        }
        if principal_locked(content, financial, permit, self).await? != principal {
            return Err(Deny::Hold(HoldCode::RequesterAccessLost));
        }
        self.lock_job(content).await
    }

    async fn lock_job(&self, content: &mut Transaction<'_, Postgres>) -> Result<(), Deny> {
        let Some(job) = &self.job else {
            return Ok(());
        };
        let owned: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM jobs WHERE id=$1 AND account_id=$2 AND kind=$3 AND attempts=$4 \
               AND status::text='running' FOR UPDATE",
        )
        .bind(job.id)
        .bind(self.account_id)
        .bind(&job.kind)
        .bind(job.attempts)
        .fetch_optional(&mut **content)
        .await
        .map_err(unavailable)?;
        owned.map(|_| ()).ok_or(Deny::Hold(HoldCode::Fenced))
    }
}

/// An explicit attributed user is charged only while they are a current,
/// unrevoked member who can still reach the graph.
async fn attributed_locked(
    content: &mut Transaction<'_, Postgres>,
    financial: Option<&mut Transaction<'_, Postgres>>,
    permit: &PlanePermit,
    work: &EnrichmentWork,
    user: Uuid,
) -> Result<Uuid, Deny> {
    let graph: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM graphs WHERE id=$1 AND account_id=$2 AND archived_at IS NULL FOR SHARE",
    )
    .bind(work.graph_id)
    .bind(work.account_id)
    .fetch_optional(&mut **content)
    .await
    .map_err(unavailable)?;
    if graph.is_none()
        || !embedding::member_reaches_locked(content, financial, permit, work.graph_id, user).await?
    {
        return Err(Deny::Hold(HoldCode::RequesterAccessLost));
    }
    Ok(user)
}

/// Policy, then the principal: the explicit attributed user, else the graph
/// executor (the selection cloud embedding uses).
async fn principal_locked(
    content: &mut Transaction<'_, Postgres>,
    mut financial: Option<&mut Transaction<'_, Postgres>>,
    permit: &PlanePermit,
    work: &EnrichmentWork,
) -> Result<Uuid, Deny> {
    if permit.account() != work.account_id {
        return Err(Deny::Unavailable);
    }
    embedding::policy(content, financial.as_deref_mut(), work.account_id).await?;
    match work.attributed_user {
        Some(user) => attributed_locked(content, financial, permit, work, user).await,
        None => embedding::executor(content, financial, permit, work.graph_id).await,
    }
}

async fn financial_tx<'a>(
    tenant: &TenantId,
    control: &'a PgPool,
    account: Uuid,
) -> Result<Option<Transaction<'a, Postgres>>, Deny> {
    match tenant {
        TenantId::Consumer => Ok(None),
        TenantId::Org(_) => Ok(Some(
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                crate::tenancy::scope::begin_account_scoped(control, account),
            )
            .await
            .map_err(unavailable)?
            .map_err(unavailable)?,
        )),
    }
}

/// Selects the member this call is charged to (step 3 of the module doc).
/// A `Hold` names why no member can be charged; the worker records it and
/// does not send.
pub(crate) async fn select_principal(
    plane: &JobScope<'_>,
    control: &PgPool,
    tenant: &TenantId,
    work: &EnrichmentWork,
) -> Result<Uuid, Deny> {
    let mut financial = financial_tx(tenant, control, work.account_id).await?;
    let mut content = embedding::begin(plane).await?;
    let selected = match embedding::admit_locked(&mut content, financial.as_mut(), tenant, work.account_id).await {
        Ok(permit) => principal_locked(&mut content, financial.as_mut(), &permit, work).await,
        Err(deny) => Err(deny),
    };
    if let Some(financial) = financial {
        financial.commit().await.map_err(unavailable)?;
    }
    content.commit().await.map_err(unavailable)?;
    selected
}

/// One metered `managed_enrichment` call for `work`, charged to `principal`
/// (from [`select_principal`]). A thin wrapper so every consumer opens the
/// gate the same way: never an aside call, the managed denial returned
/// unchanged so a hold stops the caller instead of falling into a second
/// paid call.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn complete<T: Send>(
    llm: &dyn crate::llm::LlmProvider,
    plane: &JobScope<'_>,
    control: &PgPool,
    tenant: &TenantId,
    work: &EnrichmentWork,
    principal: Uuid,
    req: crate::llm::CompletionRequest,
    usage: &crate::llm::usage::UsageContext,
    task: &str,
    validate: impl FnOnce(&crate::llm::CompletionResponse) -> T + Send,
) -> Result<(crate::llm::CompletionResponse, T), crate::llm::LlmError> {
    let call = super::ManagedCall {
        plane,
        control,
        tenant,
        account_id: work.account_id,
        principal,
        fallback_model: None,
        subject: super::Subject::Enrichment { work },
    };
    crate::llm::usage::metered_complete_managed(llm, req, usage, task, call, false, validate).await
}

/// The served receipt of a verified call. Consumed by
/// [`Verified::mark_written`] in the publish transaction.
#[derive(Debug)]
pub(crate) struct Verified {
    receipt: Uuid,
    inputs: usize,
}

impl Verified {
    pub(crate) fn receipt(&self) -> Uuid {
        self.receipt
    }

    /// Records that this call's outputs were persisted. Call it in the same
    /// transaction as the output writes.
    pub(crate) async fn mark_written(self, tx: &mut PgConnection) -> Result<(), Deny> {
        let marked = sqlx::query(
            "UPDATE managed_enrichment_inputs SET outcome='written' WHERE receipt_id=$1 AND outcome='sent'",
        )
        .bind(self.receipt)
        .execute(tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if marked != self.inputs as u64 {
            return Err(Deny::Unavailable);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum InputCheck {
    /// Every input is unchanged: persist outputs, then `mark_written`.
    Fresh(Verified),
    /// An earlier publish of this call already wrote its outputs.
    AlreadyWritten,
    /// An input changed, disappeared or was invalidated. The receipt's inputs
    /// are now `discarded_stale`; commit without outputs.
    Stale,
}

/// The publish-time re-verify (step 5 of the module doc). `current` is the
/// worker's re-read of the same inputs inside `tx`, under its own row locks;
/// a deleted input is simply absent from it. Finds the call's served receipt
/// (armed, settled), checks its recorded inputs are exactly this work's, and
/// compares them with `current`. The job, when there is one, must still be
/// owned.
pub(crate) async fn verify_inputs_locked(
    tx: &mut Transaction<'_, Postgres>,
    work: &EnrichmentWork,
    current: &[EnrichmentInput],
) -> Result<InputCheck, Deny> {
    work.lock_job(tx).await?;
    let receipt: Option<Uuid> = sqlx::query_scalar(
        "SELECT receipt_id FROM managed_attempt_receipts \
          WHERE account_id=$1 AND graph_id=$2 AND purpose=$3 AND protocol_version=1 \
            AND logical_call=$4 AND job_id IS NOT DISTINCT FROM $5 AND dispatch_phase='armed' \
            AND state IN ('billed','overrun','ambiguous') \
          ORDER BY attempt_no DESC LIMIT 1",
    )
    .bind(work.account_id)
    .bind(work.graph_id)
    .bind(PURPOSE.as_str())
    .bind(work.logical_call)
    .bind(work.job_id())
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(receipt) = receipt else {
        return Err(Deny::Unavailable);
    };
    let recorded: Vec<(i16, String, Uuid, i64, Vec<u8>, String)> = sqlx::query_as(
        "SELECT ordinal,input_kind,input_id,content_revision,content_sha256,outcome \
           FROM managed_enrichment_inputs WHERE receipt_id=$1 ORDER BY ordinal FOR UPDATE",
    )
    .bind(receipt)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    if recorded.len() != work.inputs.len() {
        return Err(Deny::Unavailable);
    }
    for (ordinal, (row, expected)) in recorded.iter().zip(&work.inputs).enumerate() {
        if row.0 as usize != ordinal
            || row.1 != expected.kind
            || row.2 != expected.id
            || row.3 != expected.content_revision
            || row.4.as_slice() != expected.content_sha256.as_slice()
        {
            return Err(Deny::Unavailable);
        }
    }
    if recorded.iter().all(|row| row.5 == "written") {
        return Ok(InputCheck::AlreadyWritten);
    }
    let invalidated = recorded.iter().any(|row| row.5 != "sent");
    let mut expected: Vec<&EnrichmentInput> = work.inputs.iter().collect();
    let mut actual: Vec<&EnrichmentInput> = current.iter().collect();
    expected.sort();
    actual.sort();
    if invalidated || expected != actual {
        sqlx::query(
            "UPDATE managed_enrichment_inputs SET outcome='discarded_stale' WHERE receipt_id=$1 AND outcome='sent'",
        )
        .bind(receipt)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        return Ok(InputCheck::Stale);
    }
    Ok(InputCheck::Fresh(Verified {
        receipt,
        inputs: work.inputs.len(),
    }))
}

/// Receipts that read a deleted input.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Invalidated {
    /// Calls not yet published: their inputs are now `invalidated`, so the
    /// publish re-verify reports them stale.
    pub in_flight: Vec<Uuid>,
    /// Calls whose outputs were already written from the input; the owning
    /// worker decides how to retract or regenerate them.
    pub published: Vec<Uuid>,
}

/// Deletion invalidation: one statement over the inputs table. Run it in the
/// transaction that deletes the input (system scope on the tenant plane,
/// account scope on the control plane).
pub(crate) async fn invalidate_inputs(
    conn: &mut PgConnection,
    input_kind: &str,
    input_ids: &[Uuid],
) -> Result<Invalidated, sqlx::Error> {
    let rows: Vec<(Uuid, bool)> = sqlx::query_as(
        "WITH flipped AS ( \
           UPDATE managed_enrichment_inputs SET outcome='invalidated' \
            WHERE input_kind=$1 AND input_id=ANY($2) AND outcome='sent' RETURNING receipt_id) \
         SELECT receipt_id, bool_or(outcome='written') FROM managed_enrichment_inputs \
          WHERE input_kind=$1 AND input_id=ANY($2) \
            AND (outcome='written' OR receipt_id IN (SELECT receipt_id FROM flipped)) \
          GROUP BY receipt_id ORDER BY receipt_id",
    )
    .bind(input_kind)
    .bind(input_ids)
    .fetch_all(conn)
    .await?;
    let mut out = Invalidated::default();
    for (receipt, published) in rows {
        if published {
            out.published.push(receipt);
        } else {
            out.in_flight.push(receipt);
        }
    }
    Ok(out)
}

/// Settles every receipt an earlier attempt of this job left behind (a crash
/// between reserve and settle). Call once after claiming the job and before
/// opening a new call.
pub(crate) async fn recover_job(
    plane: &JobScope<'_>,
    control: &PgPool,
    tenant: &TenantId,
    account: Uuid,
    job: Uuid,
) -> Result<(), Deny> {
    let mut tx = embedding::begin(plane).await?;
    let calls: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT logical_call FROM managed_attempt_receipts \
          WHERE account_id=$1 AND job_id=$2 AND purpose=$3 AND protocol_version=1 ORDER BY logical_call",
    )
    .bind(account)
    .bind(job)
    .bind(PURPOSE.as_str())
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    tx.commit().await.map_err(unavailable)?;
    for call in calls {
        super::gate::recover_logical_call(plane, control, tenant, account, call)
            .await
            .map_err(unavailable)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(kind: &str, n: u8) -> EnrichmentInput {
        EnrichmentInput::new(kind, Uuid::from_bytes([n; 16]), i64::from(n), &[n])
    }

    #[test]
    fn switch_is_off_unless_exactly_true() {
        for value in [None, Some(""), Some("false"), Some("1"), Some("TRUE"), Some("yes"), Some(" true")] {
            assert!(!switch_on(value), "{value:?} must leave enrichment off");
        }
        assert!(switch_on(Some("true")));
    }

    #[test]
    fn consumer_switch_is_on_unless_explicitly_off() {
        for value in [None, Some(""), Some("true"), Some("1"), Some("yes"), Some("anything")] {
            assert!(consumer_switch(value), "{value:?} leaves the consumer on");
        }
        for value in [Some("false"), Some("FALSE"), Some(" 0 "), Some("off"), Some("No")] {
            assert!(!consumer_switch(value), "{value:?} turns the consumer off");
        }
    }

    #[test]
    fn next_utc_day_is_after_midnight() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-19T23:30:00Z").unwrap().with_timezone(&chrono::Utc);
        assert_eq!(secs_until_next_utc_day(now), 31 * 60);
        let early = chrono::DateTime::parse_from_rfc3339("2026-10-19T00:00:30Z").unwrap().with_timezone(&chrono::Utc);
        assert_eq!(secs_until_next_utc_day(early), 24 * 3600 + 30);
    }

    #[test]
    fn work_validates_inputs() {
        let ok = |inputs| EnrichmentWork::new(Uuid::nil(), Uuid::nil(), None, None, inputs);
        assert_eq!(ok(vec![]).unwrap_err(), InvalidWork::InputCount);
        assert_eq!(ok((0..=MAX_INPUTS).map(|i| EnrichmentInput::new("atom", Uuid::new_v4(), i as i64, b"x")).collect()).unwrap_err(), InvalidWork::InputCount);
        assert!(ok((0..MAX_INPUTS).map(|i| EnrichmentInput::new("atom", Uuid::new_v4(), i as i64, b"x")).collect()).is_ok());
        assert_eq!(ok(vec![input("Atom", 1)]).unwrap_err(), InvalidWork::InputKind);
        assert_eq!(ok(vec![input("1atom", 1)]).unwrap_err(), InvalidWork::InputKind);
        assert_eq!(ok(vec![input(&"a".repeat(49), 1)]).unwrap_err(), InvalidWork::InputKind);
        assert!(ok(vec![input(&"a".repeat(48), 1)]).is_ok());
        let mut negative = input("atom", 1);
        negative.content_revision = -1;
        assert_eq!(ok(vec![negative]).unwrap_err(), InvalidWork::Revision);
        assert_eq!(ok(vec![input("atom", 1), input("atom", 1)]).unwrap_err(), InvalidWork::Duplicate);
        assert!(ok(vec![input("atom", 1), input("node", 1)]).is_ok(), "the same id under two kinds is two inputs");
    }

    #[test]
    fn binding_covers_every_input_and_the_payer() {
        let account = Uuid::new_v4();
        let graph = Uuid::new_v4();
        let job = EnrichmentJob { id: Uuid::new_v4(), kind: "EntityResolution".into(), attempts: 1 };
        let work = EnrichmentWork::new(account, graph, Some(job), None, vec![input("atom", 1), input("node", 2)]).unwrap();
        let payer = Uuid::new_v4();
        let base = work.binding(&TenantId::Consumer, payer);
        assert_eq!(base["purpose"], "managed_enrichment");
        assert_eq!(base["inputs"].as_array().unwrap().len(), 2);
        assert_ne!(base, work.binding(&TenantId::Org("org_x".into()), payer), "plane is bound");
        assert_ne!(base, work.binding(&TenantId::Consumer, Uuid::new_v4()), "payer is bound");
        for change in 0..3 {
            let mut changed = work.clone();
            match change {
                0 => changed.inputs[1].content_revision += 1,
                1 => changed.inputs[1].content_sha256[0] ^= 1,
                _ => changed.inputs[0].id = Uuid::new_v4(),
            }
            assert_ne!(base, changed.binding(&TenantId::Consumer, payer), "change {change} must alter the request hash");
        }
    }

    #[test]
    fn attributed_user_pins_the_charged_principal() {
        let user = Uuid::new_v4();
        let work = EnrichmentWork::new(Uuid::nil(), Uuid::nil(), None, Some(user), vec![input("journal_hour", 1)]).unwrap();
        assert!(work.check_call(Uuid::nil(), user).is_ok());
        assert_eq!(work.check_call(Uuid::nil(), Uuid::new_v4()), Err(Deny::Unavailable));
        assert_eq!(work.check_call(Uuid::new_v4(), user), Err(Deny::Unavailable));
        let executor = EnrichmentWork::new(Uuid::nil(), Uuid::nil(), None, None, vec![input("atom", 1)]).unwrap();
        assert!(executor.check_call(Uuid::nil(), Uuid::new_v4()).is_ok());
    }
}
