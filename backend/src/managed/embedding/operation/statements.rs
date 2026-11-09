//! Statement embeddings for scoped Ask's statement-vector leg, on the
//! managed `cloud_embedding` purpose.
//!
//! The same admission, receipts and settlement as chunk embedding
//! ([`super::process`]): the account policy and executor, the per-attempt
//! cap, the account's daily `cloud_embedding` budget, one
//! `managed_attempt_receipts` row per provider attempt and one
//! `managed_embedding_inputs` row (subject `statement`) per statement sent,
//! all durable before the send. Only the statement text of a semantic atom
//! whose source item is still permitted is sent. Vectors land in
//! `semantic_atom_embeddings`, tagged [`EMBED_MODEL_CLOUD`] (the chunk model)
//! and the statement's sha256, after a locked freshness check; a held batch
//! records `held_reason` and a `retry_after` the backfill sweep honours.

use super::*;
use coralswarm_core::embed::{EMBED_DIM, EMBED_MODEL_CLOUD};

/// Statements per provider call. Statements are short (median ~100 bytes),
/// so one call carries far more of them than the 8-chunk batch.
pub(crate) const STATEMENT_BATCH: usize = 64;
/// Statements one job sends at most; a longer version re-enqueues itself.
pub(crate) const STATEMENT_JOB_MAX: i64 = 512;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, sqlx::FromRow)]
pub(in crate::managed) struct StatementInput {
    semantic_atom_id: Uuid,
    graph_id: Uuid,
    source_version_id: Uuid,
    source_item_id: Uuid,
    /// sha256 of the stored statement: the staleness key the reader checks.
    statement_sha256: Vec<u8>,
    #[serde(skip)]
    #[sqlx(skip)]
    pub(super) text: String,
}

#[derive(sqlx::FromRow)]
struct StatementRow {
    #[sqlx(flatten)]
    input: StatementInput,
    statement: String,
}

/// The text sent and receipted: the whole statement. Size is the embedding
/// gateway's to bound; a statement over the provider's input limit is split
/// there and its vector pooled, never cut down here.
fn sent_text(statement: &str) -> String {
    statement.to_owned()
}

/// Statements of one version that need a vector: no current-model vector for
/// the current statement text, and not held past now. Only semantic atoms of
/// a permitted source item in this account's graph.
async fn load(
    tx: &mut Transaction<'_, Postgres>,
    account: Uuid,
    graph: Uuid,
    version: Uuid,
    limit: i64,
) -> Result<Vec<StatementInput>, Deny> {
    let rows: Vec<StatementRow> = sqlx::query_as(
        "SELECT a.semantic_atom_id,a.graph_id,a.source_version_id,v.source_item_id,\
          sha256(convert_to(a.statement,'UTF8')) statement_sha256,a.statement \
         FROM semantic_atoms a JOIN graphs g ON g.id=a.graph_id \
         JOIN source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id \
         JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id \
         WHERE g.account_id=$1 AND a.graph_id=$2 AND a.source_version_id=$3 AND s.access_state='permitted' \
          AND NOT EXISTS(SELECT 1 FROM semantic_atom_embeddings e WHERE e.graph_id=a.graph_id \
           AND e.semantic_atom_id=a.semantic_atom_id AND ((e.embedding IS NOT NULL AND e.embed_model=$4 \
            AND e.statement_sha256=sha256(convert_to(a.statement,'UTF8'))) OR e.retry_after>now())) \
         ORDER BY a.ordinal,a.semantic_atom_id LIMIT $5",
    )
    .bind(account)
    .bind(graph)
    .bind(version)
    .bind(EMBED_MODEL_CLOUD)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let mut input = row.input;
            input.text = sent_text(&row.statement);
            input
        })
        .collect())
}

impl PreparedBatch {
    pub(super) fn statement_worst_case(
        &self,
        sheet: &PriceSheet,
        as_of: DateTime<Utc>,
    ) -> Result<WorstCase, RefuseCode> {
        let batch = ReceiptBatch::checked_up_to(
            self.account_id,
            self.graph_id,
            self.job_id,
            self.principal,
            self.statements
                .iter()
                .map(|s| ReceiptInput {
                    atom_id: s.semantic_atom_id,
                    content_revision: 1,
                    text: s.text.clone(),
                })
                .collect(),
            STATEMENT_BATCH,
        )
        .map_err(|_| RefuseCode::CallCostCap)?;
        embedding_worst_case(&batch, sheet, as_of)
    }

    pub(super) fn statement_request_hash(
        &self,
        tenant: &TenantId,
        worst: &WorstCase,
    ) -> Result<String, Deny> {
        let inputs:Vec<_>=self.statements.iter().map(|s|serde_json::json!({
            "semantic_atom":s.semantic_atom_id,"version":s.source_version_id,"item":s.source_item_id,
            "statement_sha256":hex::encode(&s.statement_sha256),"sha256":hex::encode(Sha256::digest(s.text.as_bytes())),"bytes":s.text.len()
        })).collect();
        let plane = match tenant {
            TenantId::Consumer => "consumer".to_owned(),
            TenantId::Org(org) => format!("org:{org}"),
        };
        let bytes=serde_json::to_vec(&serde_json::json!({"purpose":"cloud_embedding","subject":"statement","plane":plane,
            "account":self.account_id,"graph":self.graph_id,"job":self.job_id,"kind":self.job_kind,"payload":self.job_payload,
            "attempts":self.job_attempts,"principal":self.principal,"inputs":inputs,"model":worst.terms.models,"ceiling":worst.ceiling().prompt,"bound":worst.per_attempt.get()})).map_err(unavailable)?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }

    pub(super) async fn insert_statement_inputs(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        receipt: Uuid,
    ) -> Result<(), Deny> {
        for (ordinal, input) in self.statements.iter().enumerate() {
            sqlx::query("INSERT INTO managed_embedding_inputs(receipt_id,account_id,graph_id,atom_id,ordinal,content_revision,content_sha256,input_bytes,subject) VALUES($1,$2,$3,$4,$5,1,$6,$7,'statement')")
                .bind(receipt).bind(self.account_id).bind(self.graph_id).bind(input.semantic_atom_id).bind(ordinal as i16)
                .bind(Sha256::digest(input.text.as_bytes()).to_vec()).bind(input.text.len() as i64)
                .execute(&mut **tx).await.map_err(unavailable)?;
        }
        Ok(())
    }

    pub(super) async fn statement_snapshot_locked(
        &self,
        content: &mut Transaction<'_, Postgres>,
    ) -> Result<LockedSnapshot, Deny> {
        statement_snapshot_locked(content, self.graph_id, &self.statements).await
    }

    pub(super) async fn hold_statements(&self, scope: &JobScope<'_>, code: &str) -> Result<(), Deny> {
        let mut tx = begin(scope).await?;
        self.lock_job(&mut tx).await?;
        write_statement_hold_locked(&mut tx, self.graph_id, &self.statements, code).await?;
        tx.commit().await.map_err(unavailable)
    }

    async fn publish_statements(
        &self,
        scope: &JobScope<'_>,
        control_pool: &PgPool,
        tenant: &TenantId,
        logical_call: Uuid,
        request_hash: &str,
        vectors: &[Vec<f32>],
    ) -> Result<Publication, Deny> {
        let mut financial = control(tenant, control_pool, self.account_id).await?;
        let mut tx = begin(scope).await?;
        self.lock_job(&mut tx).await?;
        let receipt:Option<Uuid>=sqlx::query_scalar("SELECT receipt_id FROM managed_attempt_receipts WHERE account_id=$1 AND graph_id=$2 AND job_id=$3 AND purpose='cloud_embedding' AND protocol_version=1 AND logical_call=$4 AND request_hash=$5 AND dispatch_phase='armed' AND state IN ('billed','overrun','ambiguous')")
            .bind(self.account_id).bind(self.graph_id).bind(self.job_id).bind(logical_call).bind(request_hash).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        let Some(receipt) = receipt else {
            return Err(Deny::Unavailable);
        };
        let inputs:Vec<(Uuid,i16,i64,Vec<u8>,i64,String,String)>=sqlx::query_as("SELECT atom_id,ordinal,content_revision,content_sha256,input_bytes,outcome,subject FROM managed_embedding_inputs WHERE receipt_id=$1 AND account_id=$2 AND graph_id=$3 AND purpose='cloud_embedding' ORDER BY ordinal FOR UPDATE")
            .bind(receipt).bind(self.account_id).bind(self.graph_id).fetch_all(&mut *tx).await.map_err(unavailable)?;
        if inputs.len() != self.statements.len() || vectors.len() != self.statements.len() {
            return Err(Deny::Unavailable);
        }
        for (ordinal, (actual, expected)) in inputs.iter().zip(&self.statements).enumerate() {
            if actual.0 != expected.semantic_atom_id
                || actual.1 != ordinal as i16
                || actual.2 != 1
                || actual.3 != Sha256::digest(expected.text.as_bytes()).to_vec()
                || actual.4 != expected.text.len() as i64
                || actual.6 != "statement"
            {
                return Err(Deny::Unavailable);
            }
        }
        if inputs.iter().all(|p| p.5 == "written") {
            return Ok(Publication::Written(self.statements.len()));
        }
        if inputs.iter().any(|p| p.5 != "sent") {
            return Ok(Publication::DiscardedStale);
        }
        if vectors.iter().any(|v| v.len() != EMBED_DIM || crate::store::sync::is_zero_vector(v) || v.iter().any(|x| !x.is_finite())) {
            return Err(Deny::Unavailable);
        }
        // Publish-time authority: the plane re-resolved in this transaction.
        let authority = match admit_locked(&mut tx, financial.as_mut(), tenant, self.account_id).await {
            Ok(permit) => self.authority_locked(&mut tx, financial.as_mut(), &permit).await.map(|()| permit),
            Err(deny) => Err(deny),
        };
        if matches!(authority, Err(Deny::Unavailable)) {
            return Err(Deny::Unavailable);
        }
        let snapshot = match self.statement_snapshot_locked(&mut tx).await {
            Ok(snapshot) => Some(snapshot),
            Err(Deny::Hold(_) | Deny::Refuse(_)) => None,
            Err(error) => return Err(error),
        };
        let fresh = match (&snapshot, authority) {
            (Some(snapshot), Ok(permit)) => {
                self.current_authority_locked(&mut tx, financial.as_mut(), &permit, snapshot)
                    .await
            }
            (_, Err(deny)) => Err(deny),
            (None, Ok(_)) => Err(Deny::Hold(HoldCode::Fenced)),
        };
        match fresh {
            Ok(()) => {}
            Err(Deny::Unavailable) => return Err(Deny::Unavailable),
            Err(deny) => {
                let held = if snapshot.is_some() {
                    write_statement_hold_locked(&mut tx, self.graph_id, &self.statements, reason(deny)).await?
                } else {
                    Publication::DiscardedStale
                };
                sqlx::query("UPDATE managed_embedding_inputs SET outcome='discarded_stale' WHERE receipt_id=$1 AND outcome='sent'")
                    .bind(receipt).execute(&mut *tx).await.map_err(unavailable)?;
                if let Some(financial) = financial {
                    financial.commit().await.map_err(unavailable)?;
                }
                tx.commit().await.map_err(unavailable)?;
                return Ok(held);
            }
        }
        let ids: Vec<Uuid> = self.statements.iter().map(|s| s.semantic_atom_id).collect();
        let hashes: Vec<Vec<u8>> = self.statements.iter().map(|s| s.statement_sha256.clone()).collect();
        let vecs: Vec<pgvector::Vector> = vectors.iter().cloned().map(pgvector::Vector::from).collect();
        sqlx::query("INSERT INTO semantic_atom_embeddings(graph_id,semantic_atom_id,embed_model,statement_sha256,embedding,receipt_id) \
             SELECT $1,v.id,$2,v.sha,v.embedding,$3 FROM unnest($4::uuid[],$5::bytea[],$6::vector[]) v(id,sha,embedding) \
             ON CONFLICT (graph_id,semantic_atom_id) DO UPDATE SET embed_model=EXCLUDED.embed_model,statement_sha256=EXCLUDED.statement_sha256,\
              embedding=EXCLUDED.embedding,receipt_id=EXCLUDED.receipt_id,held_reason=NULL,attempts=0,retry_after=NULL,updated_at=now()")
            .bind(self.graph_id).bind(EMBED_MODEL_CLOUD).bind(receipt).bind(&ids).bind(&hashes).bind(&vecs)
            .execute(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("UPDATE managed_embedding_inputs SET outcome='written' WHERE receipt_id=$1 AND outcome='sent'")
            .bind(receipt).execute(&mut *tx).await.map_err(unavailable)?;
        if let Some(financial) = financial {
            financial.commit().await.map_err(unavailable)?;
        }
        if let Err(error) = tx.commit().await {
            tracing::warn!(%error,%receipt,"statement embedding publication commit response uncertain; reading durable input outcomes");
            let mut read = begin(scope).await?;
            let outcomes:Vec<String>=sqlx::query_scalar("SELECT outcome FROM managed_embedding_inputs WHERE receipt_id=$1 AND account_id=$2 AND graph_id=$3 ORDER BY ordinal")
                .bind(receipt).bind(self.account_id).bind(self.graph_id).fetch_all(&mut *read).await.map_err(unavailable)?;
            read.commit().await.map_err(unavailable)?;
            if outcomes.len() != self.statements.len() || outcomes.iter().any(|o| o != "written") {
                return Err(Deny::Unavailable);
            }
        }
        Ok(Publication::Written(ids.len()))
    }
}

/// The statements, their versions and items are still what was sent:
/// every item permitted, every version present, every statement unchanged.
/// Locks them so access cannot change between this check and the write.
async fn statement_snapshot_locked(
    content: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    inputs: &[StatementInput],
) -> Result<LockedSnapshot, Deny> {
    let items: Vec<Uuid> = inputs.iter().map(|s| s.source_item_id).collect::<BTreeSet<_>>().into_iter().collect();
    let versions: Vec<Uuid> = inputs.iter().map(|s| s.source_version_id).collect::<BTreeSet<_>>().into_iter().collect();
    let permitted: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT source_item_id FROM source_items WHERE graph_id=$1 AND source_item_id=ANY($2) AND access_state='permitted' ORDER BY source_item_id FOR SHARE) s")
        .bind(graph).bind(&items).fetch_one(&mut **content).await.map_err(unavailable)?;
    if permitted != items.len() as i64 {
        return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
    }
    let present: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT version_id FROM source_versions WHERE graph_id=$1 AND version_id=ANY($2) ORDER BY version_id FOR SHARE) v")
        .bind(graph).bind(&versions).fetch_one(&mut **content).await.map_err(unavailable)?;
    if present != versions.len() as i64 {
        return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
    }
    let ids: Vec<Uuid> = inputs.iter().map(|s| s.semantic_atom_id).collect();
    let current: Vec<(Uuid, Uuid, Vec<u8>)> = sqlx::query_as("SELECT semantic_atom_id,source_version_id,sha256(convert_to(statement,'UTF8')) FROM semantic_atoms WHERE graph_id=$1 AND semantic_atom_id=ANY($2) ORDER BY semantic_atom_id FOR SHARE")
        .bind(graph).bind(&ids).fetch_all(&mut **content).await.map_err(unavailable)?;
    if current.len() != inputs.len() {
        return Err(Deny::Hold(HoldCode::SourceRemovedOrAccessLost));
    }
    for input in inputs {
        let matches = current.iter().any(|(id, version, sha)| {
            *id == input.semantic_atom_id && *version == input.source_version_id && *sha == input.statement_sha256
        });
        if !matches {
            return Err(Deny::Hold(HoldCode::Fenced));
        }
    }
    let paused: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id AND u.graph_id=r.graph_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=ANY($2) AND u.execution_owner AND r.retired_at IS NULL AND r.status='paused' AND r.pause_reason='operator_paused')")
        .bind(graph).bind(&versions).fetch_one(&mut **content).await.map_err(unavailable)?;
    Ok(LockedSnapshot { operator_paused: paused })
}

/// Records a hold: the statement keeps any vector it has, gains
/// `held_reason`, and is retried after `retry_after` (the next UTC day for
/// an exhausted daily budget, never for a permanent failure
/// (`embedding::PERMANENT_HOLDS`), else an exponential backoff capped at a
/// day).
/// Statements that no longer exist are skipped.
async fn write_statement_hold_locked(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    inputs: &[StatementInput],
    code: &str,
) -> Result<Publication, Deny> {
    let ids: Vec<Uuid> = inputs.iter().map(|s| s.semantic_atom_id).collect();
    sqlx::query("INSERT INTO semantic_atom_embeddings AS e(graph_id,semantic_atom_id,embed_model,statement_sha256,held_reason,attempts,retry_after) \
         SELECT a.graph_id,a.semantic_atom_id,$3,sha256(convert_to(a.statement,'UTF8')),$4,1,\
          CASE WHEN $4='daily_budget_exhausted' THEN (date_trunc('day',clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')+interval '1 day' \
           WHEN $4=ANY($5) THEN 'infinity'::timestamptz ELSE clock_timestamp()+interval '60 seconds' END \
         FROM semantic_atoms a WHERE a.graph_id=$1 AND a.semantic_atom_id=ANY($2) \
         ON CONFLICT (graph_id,semantic_atom_id) DO UPDATE SET held_reason=EXCLUDED.held_reason,attempts=e.attempts+1,\
          retry_after=CASE WHEN EXCLUDED.held_reason='daily_budget_exhausted' OR EXCLUDED.held_reason=ANY($5) THEN EXCLUDED.retry_after \
           ELSE clock_timestamp()+LEAST(interval '1 day',power(2,LEAST(e.attempts+1,12))*interval '60 seconds') END,updated_at=now()")
        .bind(graph).bind(&ids).bind(EMBED_MODEL_CLOUD).bind(code).bind(crate::embedding::PERMANENT_HOLDS.to_vec())
        .execute(&mut **tx).await.map_err(unavailable)?;
    Ok(Publication::Held)
}

/// One `EmbedStatements` job: the version's statements that need a vector,
/// in batches of [`STATEMENT_BATCH`], each admitted, receipted and settled
/// on the managed `cloud_embedding` purpose. Returns whether more than
/// [`STATEMENT_JOB_MAX`] were pending, so the caller re-enqueues.
pub(crate) async fn process_statements(
    tenant: &TenantId,
    pool: &PgPool,
    control_pool: &PgPool,
    job: &JobRow,
    cloud: &dyn EmbedProvider,
) -> Result<(EmbeddingReport, bool), Deny> {
    let account = job.account_id.ok_or(Deny::Unavailable)?;
    let Ok(crate::jobs::JobKind::EmbedStatements { graph_id: graph, statement_version_id: version }) =
        serde_json::from_value::<crate::jobs::JobKind>(serde_json::json!({"kind":job.kind,"payload":job.payload}))
    else {
        return Err(Deny::Unavailable);
    };
    let scope = JobScope::for_job(tenant, pool, account);
    let owner = JobOwner::acquire(&scope, job).await?;
    recover_job(&scope, control_pool, tenant, &owner).await?;
    let mut tx = begin(&scope).await?;
    let mut inputs = load(&mut tx, account, graph, version, STATEMENT_JOB_MAX + 1).await?;
    tx.commit().await.map_err(unavailable)?;
    let more = inputs.len() as i64 > STATEMENT_JOB_MAX;
    inputs.truncate(STATEMENT_JOB_MAX as usize);
    let mut report = EmbeddingReport { written: 0, discarded_stale: 0, blocked: 0, owner };
    let batch_of = |statements: Vec<StatementInput>, payload: serde_json::Value| PreparedBatch {
        account_id: account,
        graph_id: graph,
        job_id: job.id,
        principal: Uuid::nil(),
        job_kind: job.kind.clone(),
        job_payload: payload,
        job_attempts: job.attempts,
        inputs: Vec::new(),
        statements,
    };
    // The gateway bounds and groups the statements; each planned request is
    // admitted, receipted and sent on its own gate (see `super::process`).
    let texts: Vec<&str> = inputs.iter().map(|s| s.text.as_str()).collect();
    let plan = crate::embedding::plan(&texts, STATEMENT_BATCH);
    drop(texts);
    let mut slots: Vec<Option<StatementInput>> = inputs.into_iter().map(Some).collect();
    for (item, failure) in &plan.refused {
        let Some(statement) = slots[*item].take() else { continue };
        tracing::warn!(%failure,job=%job.id,"statement embedding input refused before sending");
        batch_of(vec![statement], report.owner.payload.clone()).hold_statements(&scope, crate::embedding::hold_code(failure)).await?;
        report.blocked += 1;
    }
    let mut queue: std::collections::VecDeque<(crate::embedding::Request, Vec<StatementInput>)> = plan
        .requests
        .into_iter()
        .map(|request| {
            let statements = request.items().iter().filter_map(|i| slots[*i].take()).collect();
            (request, statements)
        })
        .collect();
    while let Some((request, chunk)) = queue.pop_front() {
        report.owner.check(&scope).await?;
        let mut financial = control(tenant, control_pool, account).await?;
        let mut tx = begin(&scope).await?;
        let selected = match policy(&mut tx, financial.as_mut(), account).await {
            Ok(()) => match admit_locked(&mut tx, financial.as_mut(), tenant, account).await {
                Ok(permit) => executor(&mut tx, financial.as_mut(), &permit, graph).await,
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        if let Some(financial) = financial {
            financial.commit().await.map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        let count = chunk.len();
        let mut batch = batch_of(chunk, report.owner.payload.clone());
        batch.principal = match selected {
            Ok(principal) => principal,
            Err(Deny::Unavailable) => return Err(Deny::Unavailable),
            Err(deny) => {
                batch.hold_statements(&scope, reason(deny)).await?;
                report.blocked += count;
                continue;
            }
        };
        let call = super::super::super::ManagedCall {
            plane: &scope,
            control: control_pool,
            tenant,
            account_id: account,
            principal: batch.principal,
            fallback_model: None,
            subject: super::super::super::Subject::EmbeddingBatch { batch: &batch },
        };
        let gate = match super::super::super::ManagedGate::embedding(call).await {
            Ok(gate) => gate,
            Err(deny) => {
                batch.hold_statements(&scope, reason(deny)).await?;
                report.blocked += count;
                continue;
            }
        };
        let sent = crate::embedding::send(cloud, crate::embedding::Purpose::Managed(&gate), &request).await;
        match sent {
            Ok(vectors) => {
                report.owner.check(&scope).await?;
                let (logical_call, request_hash) = gate.embedding_binding();
                match batch.publish_statements(&scope, control_pool, tenant, logical_call, request_hash, &vectors).await? {
                    Publication::Written(written) => report.written += written,
                    Publication::Held => report.blocked += count,
                    Publication::DiscardedStale => report.discarded_stale += count,
                }
            }
            Err(failure) => match failure.error {
                EmbedError::Denied(deny) => {
                    batch.hold_statements(&scope, reason(deny)).await?;
                    report.blocked += count;
                }
                // Pin a permanent refusal on the statement at fault: each is
                // sent again alone, on its own receipt.
                _ if crate::embedding::refused_unsent(&failure) && count > 1 => {
                    tracing::warn!(%failure,job=%job.id,inputs=count,"statement embedding request refused; re-sending each statement alone");
                    drop(gate);
                    for statement in batch.statements {
                        let mut alone = crate::embedding::plan(&[statement.text.as_str()], 1);
                        match alone.requests.pop() {
                            Some(request) => queue.push_back((request, vec![statement])),
                            None => {
                                batch_of(vec![statement], report.owner.payload.clone()).hold_statements(&scope, crate::embedding::PROVIDER_REJECTED).await?;
                                report.blocked += 1;
                            }
                        }
                    }
                }
                _ => {
                    let code = crate::embedding::hold_code(&failure);
                    tracing::warn!(%failure,job=%job.id,code,"statement embedding failed after accounting");
                    batch.hold_statements(&scope, code).await?;
                    report.blocked += count;
                }
            },
        }
    }
    Ok((report, more))
}
