//! Durable graph-scoped inventory and bounded extraction admission.
//! Inventory never reads source text, blobs or local device keys. The ledger
//! counts holdings by kind; legacy chunks are not independent source support.
#[path = "corpus_enrichment_pg_tests.rs"]
#[cfg(test)]
mod pg_tests;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{middleware::auth::UserContext, tenancy::extractor::Tenant, AppState};
use crate::managed::standing::{self, PauseAction, RunKind};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub items: i64,
    pub characters: i64,
    pub model_calls: i64,
}
impl Budget {
    fn valid(&self) -> bool {
        (1..=1000).contains(&self.items)
            && (1..=8_000_000).contains(&self.characters)
            && (1..=10_000).contains(&self.model_calls)
    }
}
impl Default for Budget {
    fn default() -> Self {
        Self {
            items: 25,
            characters: 80_000,
            model_calls: 10,
        }
    }
}
/// A cohort run without a profile runs the active source profile.
fn profile_default() -> String {
    crate::workers::source_atoms::active_source_profile().into()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRequest {
    pub graph_id: Uuid,
    #[serde(default)]
    pub execute: bool,
    #[serde(default)]
    pub synthetic_demo: bool,
    pub budget: Option<Budget>,
    #[serde(default = "profile_default")]
    pub extraction_version: String,
    /// Declaration only: no key read, retry or upload occurs in this controller.
    #[serde(default)]
    pub local_holdings_unavailable: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    pub graph_id: Uuid,
    pub page_size: Option<i64>,
    pub budget: Option<Budget>,
}
#[derive(Debug, sqlx::FromRow)]
pub(crate) struct Run {
    pub(crate) run_id: Uuid,
    pub(crate) graph_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) mode: String,
    pub(crate) run_kind: RunKind,
    pub(crate) corpus_kind: String,
    pub(crate) status: String,
    pub(crate) extraction_version: String,
    pub(crate) cohort_cutoff: DateTime<Utc>,
    pub(crate) cohort_snapshot: String,
    pub(crate) stage: i16,
    pub(crate) cursor_time: Option<DateTime<Utc>>,
    pub(crate) cursor_id: Option<Uuid>,
    pub(crate) max_items: i64,
    pub(crate) local_holdings_declared: bool,
    pub(crate) local_inventory_reason: String,
}
fn reply(code: StatusCode, reason: &str) -> (StatusCode, Json<Value>) {
    (code, Json(json!({"error":reason})))
}
/// Profiles a new cohort run may take. `source-atoms-v1` is retired: no new
/// run is created for it (existing runs keep their profile).
fn supported(profile: &str) -> bool {
    matches!(profile, "source-atoms-v2" | "source-atoms-v3" | "meeting-atoms-v1")
}

// Run receipts retain private execution history, even in a collaborative graph.
// An administrator role in another active account never grants run authority.
pub(crate) async fn run_actor_allowed_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    run: Uuid,
    account: Uuid,
    user: &UserContext,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM corpus_enrichment_runs WHERE graph_id=$1 AND run_id=$2 AND account_id=$3 AND (requested_by=$4 OR $5))")
        .bind(graph).bind(run).bind(account).bind(user.user_id)
        .bind(user.account_id==account && crate::services::deletion::is_admin_role(&user.role))
        .fetch_one(&mut **tx).await
}

// Historical accounting stays durable internally. User-visible holdings and
// receipts require a current readable representation and permitted source.
const HOLDING_VISIBLE: &str = "((u.promoted_version_id IS NULL OR EXISTS(SELECT 1 FROM source_versions pv JOIN source_items ps ON ps.source_item_id=pv.source_item_id AND ps.graph_id=pv.graph_id WHERE pv.graph_id=u.graph_id AND pv.version_id=u.promoted_version_id AND ps.access_state='permitted')) AND (u.kind='local_holdings' OR (u.kind='retained_version' AND EXISTS(SELECT 1 FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=u.graph_id AND v.version_id=u.holding_id AND s.access_state='permitted')) OR (u.kind='legacy_chunk' AND EXISTS(SELECT 1 FROM atoms a WHERE a.graph_id=u.graph_id AND a.atom_id=u.holding_id AND a.atom_id IN(SELECT atom_id FROM allowed_atoms))) OR (u.kind='retained_transcript' AND EXISTS(SELECT 1 FROM meeting_transcripts t WHERE t.graph_id=u.graph_id AND t.id=u.holding_id AND (t.source_item_id IS NULL OR EXISTS(SELECT 1 FROM source_items s WHERE s.graph_id=t.graph_id AND s.source_item_id=t.source_item_id AND s.access_state='permitted'))))))";

/// P2 contract readiness is checked again by every managed model window.
/// Expand-only schema deliberately cannot enable corpus execution.
pub async fn lifecycle_ready_tx(tx: &mut Transaction<'_, Postgres>) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT to_regclass('nodes_graph_type_lower_title_identity_unique') IS NOT NULL AND to_regclass('nodes_graph_type_lower_title_unique') IS NULL AND to_regclass('source_retraction_outbox') IS NOT NULL AND corpus_lifecycle_delete_repair_ready()")
        .fetch_one(&mut **tx).await
}

pub async fn create_run(
    State(_state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(req): Json<CreateRequest>,
) -> (StatusCode, Json<Value>) {
    let account = match crate::source_evidence::authorize(&tenant, &user, req.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };
    if !supported(&req.extraction_version) || (req.execute && req.budget.is_none()) {
        return reply(
            StatusCode::UNPROCESSABLE_ENTITY,
            "supported profile and explicit execution budget required",
        );
    }
    let budget = req.budget.unwrap_or_default();
    if !budget.valid() {
        return reply(StatusCode::UNPROCESSABLE_ENTITY, "invalid budget");
    }
    let result = async {
        let mut tx = tenant.begin_member_scoped(account).await?;
        if req.execute && !lifecycle_ready_tx(&mut tx).await? {
            return Ok::<_, crate::store::StoreError>(None);
        }
        let id: Uuid = sqlx::query_scalar("INSERT INTO corpus_enrichment_runs(graph_id,account_id,requested_by,mode,status,extraction_version,model,max_items,max_characters,max_model_calls,local_holdings_declared,corpus_kind,local_inventory_reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING run_id")
            .bind(req.graph_id).bind(account).bind(user.user_id)
            .bind(if req.execute {"execute"} else {"dry_run"}).bind(if req.execute {"running"} else {"inventory"})
            .bind(req.extraction_version).bind(crate::llm::model_config::extract_model())
            .bind(budget.items).bind(budget.characters).bind(budget.model_calls).bind(true).bind(if req.synthetic_demo {"synthetic_demo"}else{"real"}).bind(if req.local_holdings_unavailable {"device_key_read_timeout_15s_no_retry_or_upload"}else{"local_inventory_not_attested_to_graph"})
            .fetch_one(&mut *tx).await?;
        let body = describe_tx(&mut tx, req.graph_id, id).await?;
        tx.commit().await?;
        Ok(body)
    }.await;
    finish(result, StatusCode::CREATED)
}
fn finish(
    result: Result<Option<Value>, crate::store::StoreError>,
    success: StatusCode,
) -> (StatusCode, Json<Value>) {
    match result {
        Ok(Some(body)) => (success, Json(body)),
        Ok(None) => reply(
            StatusCode::CONFLICT,
            "run unavailable or lifecycle contract not ready",
        ),
        Err(e) => {
            tracing::error!(error=%e, "corpus controller storage error");
            reply(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}
fn finish_command(result: Result<(StatusCode, Option<Value>), crate::store::StoreError>) -> (StatusCode, Json<Value>) {
    match result {
        Ok((code, body)) => finish(Ok(body), code),
        Err(error) => finish(Err(error), StatusCode::OK),
    }
}

#[cfg(feature = "test-support")]
pub async fn test_standing_pause_tx(tx: &mut Transaction<'_, Postgres>, graph: Uuid, run: Uuid, action: &str) -> Result<(), sqlx::Error> {
    let action = match action {
        "operator_pause" => PauseAction::OperatorPause,
        "operator_resume" => PauseAction::OperatorResume,
        "processing_off" => PauseAction::ProcessingOff,
        "processing_on" => PauseAction::ProcessingOn,
        _ => return Err(sqlx::Error::Protocol("unknown test pause action".into())),
    };
    standing::change_pause_tx(tx, graph, run, action).await
}

/// A standing fence's run locks, exactly as the fences take them: the gate
/// shared, then the run row `FOR SHARE`, held until the caller commits.
#[cfg(feature = "test-support")]
pub async fn test_standing_fence_tx(tx: &mut Transaction<'_, Postgres>, graph: Uuid, run: Uuid) -> Result<(), sqlx::Error> {
    standing::execution::share_run_gate(tx, graph).await?;
    sqlx::query("SELECT run_id FROM corpus_enrichment_runs WHERE graph_id=$1 AND run_id=$2 FOR SHARE")
        .bind(graph).bind(run).fetch_one(&mut **tx).await?;
    Ok(())
}

#[cfg(feature = "test-support")]
pub async fn test_old_cohort_advance_tx(tx: &mut Transaction<'_, Postgres>, graph: Uuid, run: Uuid, limit: i64) -> Result<(), sqlx::Error> {
    let run: Run = sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE graph_id=$1 AND run_id=$2 FOR UPDATE")
        .bind(graph).bind(run).fetch_one(&mut **tx).await?;
    advance_tx(tx, &run, limit).await
}

pub async fn get_run(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Query(req): Query<RunRequest>,
) -> (StatusCode, Json<Value>) {
    let account = match crate::source_evidence::authorize(&tenant, &user, req.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };
    finish(
        async {
            let mut tx = tenant.begin_member_scoped(account).await?;
            if !run_actor_allowed_tx(&mut tx, req.graph_id, id, account, &user).await? {
                return Ok(None);
            }
            let body = describe_tx(&mut tx, req.graph_id, id).await?;
            tx.commit().await?;
            Ok(body)
        }
        .await,
        StatusCode::OK,
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnitsQuery {
    pub graph_id: Uuid,
    pub before: Option<Uuid>,
    pub limit: Option<i64>,
}
pub async fn get_units(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Query(req): Query<UnitsQuery>,
) -> (StatusCode, Json<Value>) {
    let account = match crate::source_evidence::authorize(&tenant, &user, req.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };
    let limit = req.limit.unwrap_or(20);
    if !(1..=100).contains(&limit) {
        return reply(StatusCode::UNPROCESSABLE_ENTITY, "invalid page size");
    }
    finish(async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        let exists=run_actor_allowed_tx(&mut tx,req.graph_id,id,account,&user).await?;
        if !exists{return Ok(None)}
        let mut units:Vec<Value>=sqlx::query_scalar(&format!("WITH allowed_atoms AS MATERIALIZED(SELECT atom_id FROM library_permitted_atoms($2::uuid)) SELECT to_jsonb(u)||jsonb_build_object('representation',rv.representation,'jobs',COALESCE((SELECT jsonb_agg(j) FROM (SELECT id,status,attempts,run_after,finished_at,(u.execution_owner AND (finished_at IS NULL OR finished_at>=u.observed_at)) AS managed_by_this_run FROM jobs WHERE (u.kind='retained_version' OR u.promoted_version_id IS NOT NULL) AND payload->>'graph_id'=u.graph_id::text AND payload->>'source_version_id'=COALESCE(u.promoted_version_id,u.holding_id)::text AND kind IN ('ExtractMeetingAtoms','ExtractSourceAtoms') ORDER BY created_at DESC,id DESC LIMIT 10) j),'[]')) FROM corpus_enrichment_units u LEFT JOIN source_versions rv ON rv.graph_id=u.graph_id AND rv.version_id=COALESCE(u.promoted_version_id,u.holding_id) AND (u.kind='retained_version' OR u.promoted_version_id IS NOT NULL) WHERE u.run_id=$1 AND u.graph_id=$2 AND ($3::uuid IS NULL OR u.unit_id<$3) AND {HOLDING_VISIBLE} ORDER BY u.unit_id DESC LIMIT $4"))
            .bind(id).bind(req.graph_id).bind(req.before).bind(limit+1).fetch_all(&mut *tx).await?;
        let next=if units.len()>limit as usize {units.truncate(limit as usize);units.last().map(|u|u["unit_id"].clone())}else{None};
        tx.commit().await?;
        Ok(Some(json!({"graph_id":req.graph_id,"run_id":id,"units":units,"next_cursor":next})))
    }.await,StatusCode::OK)
}
pub(crate) async fn describe_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    id: Uuid,
) -> Result<Option<Value>, sqlx::Error> {
    let row: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(r) FROM corpus_enrichment_runs r WHERE graph_id=$1 AND run_id=$2",
    )
    .bind(graph)
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(mut body) = row else { return Ok(None) };
    body["coverage"]=sqlx::query_scalar::<_,Value>(&format!("WITH allowed_atoms AS MATERIALIZED(SELECT atom_id FROM library_permitted_atoms($1::uuid)) SELECT COALESCE(jsonb_agg(c),'[]') FROM (SELECT u.kind,u.state,u.reason,u.original_availability,rv.representation,u.processing_path,count(*) AS holding_count,CASE WHEN u.kind='retained_version' THEN count(DISTINCT u.source_item_id) ELSE NULL END AS source_item_count FROM corpus_enrichment_units u LEFT JOIN source_versions rv ON rv.graph_id=u.graph_id AND rv.version_id=COALESCE(u.promoted_version_id,u.holding_id) AND (u.kind='retained_version' OR u.promoted_version_id IS NOT NULL) WHERE u.graph_id=$1 AND u.run_id=$2 AND {HOLDING_VISIBLE} GROUP BY u.kind,u.state,u.reason,u.original_availability,rv.representation,u.processing_path ORDER BY u.kind,u.state,u.reason,u.original_availability,rv.representation) c"))
        .bind(graph).bind(id).fetch_one(&mut **tx).await?;
    body["count_semantics"] = json!({"retained_version":"version_count","legacy_chunk":"chunk_count_not_source_count","retained_transcript":"projection_count","local_holdings":"declared_inventory_count_content_count_unknown"});
    Ok(Some(body))
}
pub async fn advance_run(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(req): Json<RunRequest>,
) -> (StatusCode, Json<Value>) {
    let account = match crate::source_evidence::authorize(&tenant, &user, req.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };
    let limit = req.page_size.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return reply(StatusCode::UNPROCESSABLE_ENTITY, "invalid page size");
    }
    finish_command(async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        let run:Option<Run>=sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE graph_id=$1 AND run_id=$2 FOR UPDATE")
            .bind(req.graph_id).bind(id).fetch_optional(&mut *tx).await?;
        let Some(run)=run else{return Ok((StatusCode::OK,None))};
        if !run_actor_allowed_tx(&mut tx,req.graph_id,id,account,&user).await? {return Ok((StatusCode::OK,None))}
        if run.run_kind==RunKind::Standing {
            if !crate::source_evidence::still_admitted_in_tx(&state,&tenant,&user,req.graph_id,account,&mut tx).await {return Ok((StatusCode::OK,None))}
            return Ok((StatusCode::UNPROCESSABLE_ENTITY,Some(json!({"error":"standing runs do not accept cohort advance"}))));
        }
        if run.mode=="execute" && !lifecycle_ready_tx(&mut tx).await? {
            sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason=$2 WHERE run_id=$1")
                .bind(id).bind("lifecycle_contract_not_ready").execute(&mut *tx).await?;
        } else if run.status!="paused" && run.status!="complete" {
            advance_tx(&mut tx,&run,limit).await?;
        }
        let body=describe_tx(&mut tx,req.graph_id,id).await?;
        tx.commit().await?;
        Ok((StatusCode::OK,body))
    }.await)
}
pub async fn pause_run(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(req): Json<RunRequest>,
) -> (StatusCode, Json<Value>) {
    change_status(state, tenant, user, id, req, false).await
}
pub async fn resume_run(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(req): Json<RunRequest>,
) -> (StatusCode, Json<Value>) {
    change_status(state, tenant, user, id, req, true).await
}
async fn change_status(
    state: Arc<AppState>,
    tenant: Tenant,
    user: UserContext,
    id: Uuid,
    req: RunRequest,
    resume: bool,
) -> (StatusCode, Json<Value>) {
    let account = match crate::source_evidence::authorize(&tenant, &user, req.graph_id).await {
        Ok(a) => a,
        Err(e) => return e,
    };
    if req.budget.as_ref().is_some_and(|b| !b.valid()) {
        return reply(StatusCode::UNPROCESSABLE_ENTITY, "invalid budget");
    }
    finish_command(async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        // An operator pause or resume writes the run row: the standing gate
        // comes first, or overlapping standing fences could hold it off for
        // as long as windows keep arriving (`share_run_gate`).
        standing::execution::exclusive_run_gate(&mut tx,req.graph_id).await?;
        let run:Option<Run>=sqlx::query_as("SELECT * FROM corpus_enrichment_runs WHERE graph_id=$1 AND run_id=$2 FOR UPDATE").bind(req.graph_id).bind(id).fetch_optional(&mut *tx).await?;
        let Some(run)=run else{return Ok((StatusCode::OK,None))};
        if !run_actor_allowed_tx(&mut tx,req.graph_id,id,account,&user).await? {return Ok((StatusCode::OK,None))}
        if run.run_kind==RunKind::Standing {
            if !crate::source_evidence::still_admitted_in_tx(&state,&tenant,&user,req.graph_id,account,&mut tx).await {return Ok((StatusCode::OK,None))}
            if resume && req.budget.is_some() {
                return Ok((StatusCode::UNPROCESSABLE_ENTITY,Some(json!({"error":"standing runs do not accept logical budgets"}))));
            }
            standing::change_pause_tx(&mut tx,req.graph_id,id,if resume {PauseAction::OperatorResume}else{PauseAction::OperatorPause}).await?;
            let body=describe_tx(&mut tx,req.graph_id,id).await?;
            tx.commit().await?;
            return Ok((StatusCode::OK,body));
        }
        if resume && run.mode=="execute" && !lifecycle_ready_tx(&mut tx).await? {return Ok((StatusCode::OK,None))}
        if resume {if let Some(budget)=req.budget {
            sqlx::query("UPDATE corpus_enrichment_runs SET max_items=$2,max_characters=$3,max_model_calls=$4 WHERE run_id=$1")
                .bind(id).bind(budget.items).bind(budget.characters).bind(budget.model_calls).execute(&mut *tx).await?;
        }}
        sqlx::query("UPDATE corpus_enrichment_runs SET status=$2,pause_reason=$3,updated_at=now() WHERE run_id=$1 AND status<>'complete'")
            .bind(id).bind(if resume {if run.mode=="execute"{"running"}else{"inventory"}}else{"paused"})
            .bind(if resume {None}else{Some("operator_paused")}).execute(&mut *tx).await?;
        let body=describe_tx(&mut tx,req.graph_id,id).await?;tx.commit().await?;Ok((StatusCode::OK,body))
    }.await)
}

#[derive(sqlx::FromRow)]
struct Holding {
    id: Uuid,
    observed: DateTime<Utc>,
    source_item: Option<Uuid>,
    profile: Option<String>,
    state: String,
    reason: Option<String>,
    original_availability: String,
    processing_path: String,
}
async fn advance_tx(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    limit: i64,
) -> Result<(), sqlx::Error> {
    if run.mode == "execute" {
        // A larger budget resumes declined admissions without resetting usage.
        let waiting:Vec<(Uuid,Uuid)>=sqlx::query_as("SELECT unit_id,holding_id FROM corpus_enrichment_units WHERE run_id=$1 AND kind='retained_version' AND NOT execution_owner AND state='pending' AND reason='item_admission_limit' ORDER BY observed_at,unit_id LIMIT $2")
            .bind(run.run_id).bind(limit).fetch_all(&mut **tx).await?;
        for (unit, version) in waiting {
            admit_tx(tx, run, unit, version).await?;
        }
        let paused: bool = sqlx::query_scalar(
            "SELECT status='paused' FROM corpus_enrichment_runs WHERE run_id=$1",
        )
        .bind(run.run_id)
        .fetch_one(&mut **tx)
        .await?;
        if paused {
            return Ok(());
        }
    }
    // Reconciliation is bounded and uses current RLS visibility. Missing sources
    // cannot leave completed work counted as current retained evidence.
    sqlx::query("UPDATE corpus_enrichment_units u SET state=CASE WHEN v.version_id IS NULL THEN 'unavailable' WHEN v.extraction_status='done' THEN 'processed' WHEN v.extraction_status='failed' THEN 'failed' WHEN u.state='unavailable' AND u.reason='source_removed_or_access_lost' THEN 'pending' ELSE u.state END,reason=CASE WHEN v.version_id IS NULL THEN 'source_removed_or_access_lost' WHEN v.extraction_status='done' THEN NULL WHEN v.extraction_status='failed' THEN 'extraction_failed' WHEN u.state='unavailable' AND u.reason='source_removed_or_access_lost' THEN NULL ELSE u.reason END,updated_at=now() FROM (SELECT u2.unit_id,v.version_id,v.extraction_status FROM corpus_enrichment_units u2 LEFT JOIN source_versions v ON v.version_id=COALESCE(u2.promoted_version_id,u2.holding_id) AND v.graph_id=u2.graph_id AND EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id AND s.access_state='permitted') WHERE u2.run_id=$1 AND (u2.kind='retained_version' OR u2.promoted_version_id IS NOT NULL) AND u2.state<>'excluded' ORDER BY u2.updated_at,u2.unit_id LIMIT $2) v WHERE u.unit_id=v.unit_id")
        .bind(run.run_id).bind(limit).execute(&mut **tx).await?;
    if run.stage >= 4 {
        if run.mode == "execute" {
            requeue_tx(tx, run, limit).await?;
        }
        sqlx::query("UPDATE corpus_enrichment_runs SET status=CASE WHEN mode='dry_run' THEN 'ready' WHEN NOT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE run_id=$1 AND state IN ('pending','queued','failed')) THEN 'complete' ELSE status END,updated_at=now() WHERE run_id=$1")
            .bind(run.run_id).execute(&mut **tx).await?;
        return Ok(());
    }
    if run.stage == 3 {
        if run.local_holdings_declared {
            insert_unit_tx(
                tx,
                run,
                "local_holdings",
                run.run_id,
                None,
                None,
                "unavailable",
                Some(&run.local_inventory_reason),
                "unknown",
                "local_unavailable",
            )
            .await?;
        }
        sqlx::query("UPDATE corpus_enrichment_runs SET stage=4,cursor_time=NULL,cursor_id=NULL WHERE run_id=$1").bind(run.run_id).execute(&mut **tx).await?;
        return Ok(());
    }
    let sql=match run.stage {
        0=>"SELECT version_id AS id,captured_at AS observed,source_item_id AS source_item,extraction_version AS profile,CASE WHEN COALESCE(source_metadata->>'synthetic_demo'='true' OR origin->>'transport'='synthetic',false)<>$7 THEN 'excluded' WHEN NOT EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=source_versions.source_item_id AND s.graph_id=source_versions.graph_id AND s.access_state='permitted') THEN 'unavailable' WHEN extraction_version<>$6 THEN 'excluded' WHEN raw_inline IS NULL AND raw_blob_key IS NULL THEN 'unavailable' WHEN extraction_status='done' THEN 'processed' ELSE 'pending' END AS state,CASE WHEN COALESCE(source_metadata->>'synthetic_demo'='true' OR origin->>'transport'='synthetic',false)<>$7 THEN CASE WHEN $7 THEN 'real_source_outside_demo_corpus' ELSE 'synthetic_demo_outside_real_corpus' END WHEN NOT EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=source_versions.source_item_id AND s.graph_id=source_versions.graph_id AND s.access_state='permitted') THEN 'source_removed_or_access_lost' WHEN extraction_version<>$6 THEN 'different_pinned_extraction_profile' WHEN raw_inline IS NULL AND raw_blob_key IS NULL THEN 'original_unavailable' ELSE NULL END AS reason,CASE WHEN raw_inline IS NOT NULL THEN 'available' WHEN raw_blob_key IS NOT NULL THEN 'unknown' ELSE 'unavailable' END AS original_availability,'retained_version'::text AS processing_path FROM source_versions WHERE graph_id=$1 AND captured_at<=$2 AND pg_visible_in_snapshot(inventory_created_xid,$8::pg_snapshot) AND ($3::timestamptz IS NULL OR (captured_at,version_id)>($3,$4)) ORDER BY captured_at,version_id LIMIT $5",
        1=>"WITH allowed_atoms AS MATERIALIZED(SELECT atom_id FROM library_permitted_atoms($1::uuid)) SELECT a.atom_id AS id,a.created_at AS observed,NULL::uuid AS source_item,NULL::text AS profile,CASE WHEN $7 THEN 'excluded' WHEN NOT a.atom_id IN(SELECT atom_id FROM allowed_atoms) THEN 'unavailable' WHEN a.source_evidence_bound THEN 'excluded' WHEN a.promoted_from_atom_id IS NOT NULL OR EXISTS(SELECT 1 FROM source_connection_atoms d WHERE d.target_atom_id=a.atom_id) THEN 'excluded' ELSE 'pending' END AS state,CASE WHEN $7 THEN 'unclassified_legacy_outside_demo_corpus' WHEN NOT a.atom_id IN(SELECT atom_id FROM allowed_atoms) THEN 'source_removed_or_access_lost' WHEN a.source_evidence_bound THEN 'covered_by_retained_version' WHEN a.promoted_from_atom_id IS NOT NULL OR EXISTS(SELECT 1 FROM source_connection_atoms d WHERE d.target_atom_id=a.atom_id) THEN 'derived_source_baseline_unsupported' ELSE 'retained_legacy_baseline_promotion_required' END AS reason,'unknown'::text AS original_availability,CASE WHEN a.source_evidence_bound THEN 'covered_by_retained_version' ELSE 'legacy_normalized_baseline_pending' END::text AS processing_path FROM atoms a WHERE a.graph_id=$1 AND a.created_at<=$2 AND pg_visible_in_snapshot(a.inventory_created_xid,$8::pg_snapshot) AND ($3::timestamptz IS NULL OR (a.created_at,a.atom_id)>($3,$4)) AND $6::text IS NOT NULL AND $7::bool IS NOT NULL ORDER BY a.created_at,a.atom_id LIMIT $5",

        _=>"SELECT id,created_at AS observed,source_item_id AS source_item,NULL::text AS profile,CASE WHEN current_version_id IS NOT NULL OR $7 THEN 'excluded' WHEN raw_inline IS NULL AND raw_blob_key IS NULL THEN 'unavailable' ELSE 'pending' END AS state,CASE WHEN current_version_id IS NOT NULL THEN 'covered_by_retained_version' WHEN $7 THEN 'unclassified_transcript_outside_demo_corpus' WHEN raw_inline IS NULL AND raw_blob_key IS NULL THEN 'original_unavailable' ELSE 'retained_transcript_capture_required' END AS reason,CASE WHEN raw_inline IS NOT NULL THEN 'available' WHEN raw_blob_key IS NOT NULL THEN 'unknown' ELSE 'unavailable' END AS original_availability,CASE WHEN current_version_id IS NOT NULL THEN 'covered_by_retained_version' ELSE 'retained_transcript_capture_pending' END AS processing_path FROM meeting_transcripts WHERE graph_id=$1 AND created_at<=$2 AND pg_visible_in_snapshot(inventory_created_xid,$8::pg_snapshot) AND ($3::timestamptz IS NULL OR (created_at,id)>($3,$4)) AND $6::text IS NOT NULL AND $7::bool IS NOT NULL ORDER BY created_at,id LIMIT $5",
    };
    let rows: Vec<Holding> = sqlx::query_as(sql)
        .bind(run.graph_id)
        .bind(run.cohort_cutoff)
        .bind(run.cursor_time)
        .bind(run.cursor_id)
        .bind(limit)
        .bind(&run.extraction_version)
        .bind(run.corpus_kind == "synthetic_demo")
        .bind(&run.cohort_snapshot)
        .fetch_all(&mut **tx)
        .await?;
    let kind = match run.stage {
        0 => "retained_version",
        1 => "legacy_chunk",
        _ => "retained_transcript",
    };
    for h in &rows {
        let unit = insert_unit_tx(
            tx,
            run,
            kind,
            h.id,
            h.source_item,
            h.profile.as_deref(),
            &h.state,
            h.reason.as_deref(),
            &h.original_availability,
            &h.processing_path,
        )
        .await?;
        if run.mode == "execute" && run.stage == 0 && h.state == "pending" {
            admit_tx(tx, run, unit, h.id).await?;
        }
    }
    let last = rows.last();
    sqlx::query("UPDATE corpus_enrichment_runs SET stage=$2,cursor_time=$3,cursor_id=$4,updated_at=now() WHERE run_id=$1")
        .bind(run.run_id).bind(if rows.len()<limit as usize{run.stage+1}else{run.stage})
        .bind(if rows.len()<limit as usize{None}else{last.map(|h|h.observed)})
        .bind(if rows.len()<limit as usize{None}else{last.map(|h|h.id)}).execute(&mut **tx).await?;
    Ok(())
}
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_unit_tx(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    kind: &str,
    id: Uuid,
    source: Option<Uuid>,
    profile: Option<&str>,
    state: &str,
    reason: Option<&str>,
    original_availability: &str,
    processing_path: &str,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar("INSERT INTO corpus_enrichment_units(run_id,graph_id,kind,holding_id,source_item_id,extraction_version,state,reason,original_availability,processing_path) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(run_id,kind,holding_id) DO UPDATE SET observed_at=corpus_enrichment_units.observed_at RETURNING unit_id")
        .bind(run.run_id).bind(run.graph_id).bind(kind).bind(id).bind(source).bind(profile).bind(state).bind(reason).bind(original_availability).bind(processing_path).fetch_one(&mut **tx).await
}
pub(crate) async fn admit_tx(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    unit: Uuid,
    version: Uuid,
) -> Result<(), sqlx::Error> {
    // Same-source lock serializes two runs before checking/claiming ownership.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('corpus-version:' || $1::text))")
        .bind(version.to_string())
        .execute(&mut **tx)
        .await?;
    // Capture/persistence lock parents before versions. Never lock a pending
    // job while waiting for its source: tracked capture enqueues may lock it.
    let parent:Option<Uuid>=sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND access_state='permitted' AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
        .bind(run.graph_id).bind(version).fetch_optional(&mut **tx).await?;
    if parent.is_none() {
        sqlx::query("UPDATE corpus_enrichment_units SET state='unavailable',reason='source_removed_or_access_lost' WHERE unit_id=$1").bind(unit).execute(&mut **tx).await?;
        return Ok(());
    }
    let current:Option<String>=sqlx::query_scalar(
        "SELECT extraction_status FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE",
    )
    .bind(run.graph_id)
    .bind(version)
    .fetch_optional(&mut **tx)
    .await?;
    if run.run_kind == RunKind::Standing && current.as_deref() == Some("failed") {
        sqlx::query("UPDATE corpus_enrichment_units SET state='failed',reason='extraction_failed' WHERE unit_id=$1")
            .bind(unit).execute(&mut **tx).await?;
        return Ok(());
    }
    if current.as_deref() == Some("done") {
        sqlx::query(
            "UPDATE corpus_enrichment_units SET state='processed',reason=NULL WHERE unit_id=$1",
        )
        .bind(unit)
        .execute(&mut **tx)
        .await?;
        return Ok(());
    }
    let other:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND extraction_version=$3 AND execution_owner AND unit_id<>$4)")
        .bind(run.graph_id).bind(version).bind(&run.extraction_version).bind(unit).fetch_one(&mut **tx).await?;
    let usage: i64 =
        sqlx::query_scalar("SELECT admitted_items FROM corpus_enrichment_runs WHERE run_id=$1")
            .bind(run.run_id)
            .fetch_one(&mut **tx)
            .await?;
    let existing:Vec<String>=sqlx::query_scalar("SELECT status::text FROM jobs WHERE payload->>'source_version_id'=$1 AND status::text IN ('pending','running') FOR UPDATE")
        .bind(version.to_string()).fetch_all(&mut **tx).await?;
    let outside_running = existing.iter().any(|s| s == "running");
    if other || outside_running {
        sqlx::query(
            "UPDATE corpus_enrichment_units SET state='excluded',reason=$2 WHERE unit_id=$1",
        )
        .bind(unit)
        .bind(if other {
            "owned_by_another_run"
        } else {
            "already_running_outside_run"
        })
        .execute(&mut **tx)
        .await?;
        return Ok(());
    }
    if run.run_kind == RunKind::Cohort && usage >= run.max_items {
        sqlx::query("UPDATE corpus_enrichment_units SET state='pending',reason='item_admission_limit' WHERE unit_id=$1").bind(unit).execute(&mut **tx).await?;
        sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason='item_admission_limit' WHERE run_id=$1").bind(run.run_id).execute(&mut **tx).await?;
        return Ok(());
    }
    // Bind existing jobs as well: a pending/running native job cannot bypass
    // the newly claimed run. Every window locates the owner by version/profile.
    sqlx::query("UPDATE corpus_enrichment_units SET execution_owner=true,state='queued',reason=NULL WHERE unit_id=$1").bind(unit).execute(&mut **tx).await?;
    if run.run_kind == RunKind::Cohort {
        sqlx::query("UPDATE corpus_enrichment_runs SET admitted_items=admitted_items+1 WHERE run_id=$1")
            .bind(run.run_id).execute(&mut **tx).await?;
    }
    queue_unit_tx(tx, run, version).await
}
pub(crate) async fn queue_unit_tx(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    version: Uuid,
) -> Result<(), sqlx::Error> {
    let parent:Option<Uuid>=sqlx::query_scalar("SELECT source_item_id FROM source_items WHERE graph_id=$1 AND access_state='permitted' AND source_item_id=(SELECT source_item_id FROM source_versions WHERE graph_id=$1 AND version_id=$2) FOR UPDATE")
        .bind(run.graph_id).bind(version).fetch_optional(&mut **tx).await?;
    if parent.is_none() {
        return Ok(());
    }
    sqlx::query(
        "SELECT version_id FROM source_versions WHERE graph_id=$1 AND version_id=$2 FOR UPDATE",
    )
    .bind(run.graph_id)
    .bind(version)
    .fetch_optional(&mut **tx)
    .await?;
    let cursor:Option<i64>=sqlx::query_scalar("SELECT next_start_char FROM source_versions WHERE graph_id=$1 AND version_id=$2 AND extraction_status<>'done'").bind(run.graph_id).bind(version).fetch_optional(&mut **tx).await?;
    let Some(start) = cursor else { return Ok(()) };
    if run.run_kind == RunKind::Standing {
        sqlx::query("UPDATE jobs SET kind=CASE kind WHEN 'ExtractSourceAtoms' THEN 'ExtractStandingSourceAtoms' ELSE 'ExtractStandingMeetingAtoms' END WHERE status::text='pending' AND kind IN ('ExtractSourceAtoms','ExtractMeetingAtoms') AND payload->>'graph_id'=$1 AND payload->>'source_version_id'=$2")
            .bind(run.graph_id.to_string()).bind(version.to_string()).execute(&mut **tx).await?;
        crate::source_evidence::queue_extraction_tx(tx, run.graph_id, version, run.account_id, start).await?;
        return Ok(());
    }
    let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE payload->>'source_version_id'=$1 AND status::text IN ('pending','running'))").bind(version.to_string()).fetch_one(&mut **tx).await?;
    if !active {
        crate::source_evidence::queue_extraction_tx(
            tx,
            run.graph_id,
            version,
            run.account_id,
            start,
        )
        .await?;
    }
    Ok(())
}
async fn requeue_tx(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    limit: i64,
) -> Result<(), sqlx::Error> {
    let versions:Vec<Uuid>=sqlx::query_scalar("SELECT COALESCE(promoted_version_id,holding_id) FROM corpus_enrichment_units WHERE run_id=$1 AND execution_owner AND state IN ('pending','failed') ORDER BY updated_at,unit_id LIMIT $2").bind(run.run_id).bind(limit).fetch_all(&mut **tx).await?;
    for version in versions {
        queue_unit_tx(tx, run, version).await?;
        sqlx::query("UPDATE corpus_enrichment_units SET state='queued',reason=NULL WHERE run_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND execution_owner").bind(run.run_id).bind(version).execute(&mut **tx).await?;
    }
    Ok(())
}

/// Per-attempt reservation, not per queued item. Retried failed model calls
/// consume budget again. The run row serializes concurrent window admissions.
#[derive(Debug, PartialEq, Eq)]
pub enum WindowGate {
    Permit { model: String },
    Hold,
}
pub async fn reserve_window_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    profile: &str,
    characters: i64,
    model_call: bool,
) -> Result<WindowGate, sqlx::Error> {
    let owner:Option<(Uuid,Uuid)>=sqlx::query_as("SELECT run_id,unit_id FROM corpus_enrichment_units WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND extraction_version=$3 AND execution_owner").bind(graph).bind(version).bind(profile).fetch_optional(&mut **tx).await?;
    let Some((run, unit)) = owner else {
        return Ok(WindowGate::Hold);
    };
    let row:(String,String,i64,i64,i64,i64)=sqlx::query_as("SELECT status,model,max_characters,attempted_characters,max_model_calls,attempted_model_calls FROM corpus_enrichment_runs WHERE run_id=$1 AND graph_id=$2 FOR UPDATE").bind(run).bind(graph).fetch_one(&mut **tx).await?;
    if row.0 != "running"
        || !lifecycle_ready_tx(tx).await?
        || !source_permitted_tx(tx, graph, version, profile).await?
    {
        return Ok(WindowGate::Hold);
    }
    let calls = i64::from(model_call);
    if characters < 0 || row.3 + characters > row.2 || row.5 + calls > row.4 {
        sqlx::query("UPDATE corpus_enrichment_runs SET status='paused',pause_reason='execution_budget_exhausted' WHERE run_id=$1").bind(run).execute(&mut **tx).await?;
        sqlx::query("UPDATE corpus_enrichment_units SET state='pending',reason='execution_budget_exhausted',updated_at=now() WHERE unit_id=$1").bind(unit).execute(&mut **tx).await?;
        return Ok(WindowGate::Hold);
    }
    sqlx::query("UPDATE corpus_enrichment_runs SET attempted_characters=attempted_characters+$2,attempted_model_calls=attempted_model_calls+$3,updated_at=now() WHERE run_id=$1").bind(run).bind(characters).bind(calls).execute(&mut **tx).await?;
    sqlx::query("UPDATE corpus_enrichment_units SET attempted_characters=attempted_characters+$2,attempted_model_calls=attempted_model_calls+$3,updated_at=now() WHERE unit_id=$1").bind(unit).bind(characters).bind(calls).execute(&mut **tx).await?;
    Ok(WindowGate::Permit { model: row.1 })
}
/// Shared trusted enqueue admission. No automatic run or budget is minted.
/// A retained capture without an explicit owner stays inspectable and blocked.
pub async fn enqueue_admission_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    account: Uuid,
) -> Result<bool, sqlx::Error> {
    let row:Option<(Uuid,Option<Uuid>,String,bool)>=sqlx::query_as("SELECT v.source_item_id,v.ingested_by,v.extraction_version,COALESCE(v.origin->>'transport'='synthetic',false) OR COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false) FROM source_versions v WHERE v.graph_id=$1 AND v.version_id=$2")
        .bind(graph).bind(version).fetch_optional(&mut **tx).await?;
    let Some((item, actor, profile, demo)) = row else {
        return Ok(false);
    };
    let owner:Option<(Uuid,Uuid)>=sqlx::query_as("SELECT r.run_id,u.unit_id FROM corpus_enrichment_units u JOIN corpus_enrichment_runs r ON r.run_id=u.run_id AND r.graph_id=u.graph_id JOIN source_items s ON s.source_item_id=$4 AND s.graph_id=u.graph_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 AND u.extraction_version=$3 AND u.execution_owner AND r.account_id=$5 AND r.mode='execute' AND r.status='running' AND r.extraction_version=$3 AND (r.corpus_kind='synthetic_demo')=$6 AND s.access_state='permitted'")
        .bind(graph).bind(version).bind(&profile).bind(item).bind(account).bind(demo).fetch_optional(&mut **tx).await?;
    let admitted = owner.is_some() && lifecycle_ready_tx(tx).await?;
    let reason = if admitted {
        None
    } else if actor.is_none() {
        Some("capture_actor_unknown_managed_admission_required")
    } else {
        Some("managed_admission_required")
    };
    sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,run_id,unit_id,state,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(source_version_id) DO UPDATE SET run_id=COALESCE(corpus_capture_admissions.run_id,EXCLUDED.run_id),unit_id=COALESCE(corpus_capture_admissions.unit_id,EXCLUDED.unit_id),state=CASE WHEN corpus_capture_admissions.state='processed' THEN corpus_capture_admissions.state ELSE EXCLUDED.state END,reason=CASE WHEN corpus_capture_admissions.state='processed' THEN corpus_capture_admissions.reason ELSE EXCLUDED.reason END,updated_at=now()")
        .bind(version).bind(graph).bind(account).bind(item).bind(actor).bind(profile).bind(owner.map(|o|o.0)).bind(owner.map(|o|o.1)).bind(if admitted{"queued"}else{"blocked"}).bind(reason).execute(&mut **tx).await?;
    if !admitted {
        sqlx::query("UPDATE source_versions SET extraction_status='blocked',extraction_error=$3 WHERE graph_id=$1 AND version_id=$2 AND extraction_status<>'done'")
            .bind(graph).bind(version).bind(reason).execute(&mut **tx).await?;
    }
    Ok(admitted)
}

/// The receipt of a carried capture (`source_evidence::carries_extraction`):
/// recorded `done` at capture and never queued, so [`enqueue_admission_tx`]
/// never runs for it. Without this it had no receipt and its clock read
/// `unknown` forever (tenant 0193 / control 0362). It is `processed` on
/// insert; the observation trigger records `carried_forward` only when the
/// source facts show a carry, and `unverified_transition` (red) otherwise.
/// The trigger rejects a `processed` receipt for an item that is not
/// `permitted` (and for `selected-passages-v1`, which its publication path
/// receipts itself), and a rejected receipt must not fail the capture, so
/// those carries keep no receipt and stay visibly `unknown`.
pub async fn record_carried_receipt_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    account: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO corpus_capture_admissions(source_version_id,graph_id,account_id,source_item_id,captured_by,extraction_version,state) SELECT v.version_id,v.graph_id,$3,v.source_item_id,v.ingested_by,v.extraction_version,'processed' FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=$1 AND v.version_id=$2 AND v.extraction_status='done' AND s.access_state='permitted' AND v.extraction_version<>'selected-passages-v1' ON CONFLICT(source_version_id) DO NOTHING")
        .bind(graph).bind(version).bind(account).execute(&mut **tx).await?;
    Ok(())
}

pub async fn source_permitted_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    profile: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_versions v JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE v.graph_id=$1 AND v.version_id=$2 AND v.extraction_version=$3 AND s.access_state='permitted')")
        .bind(graph).bind(version).bind(profile).fetch_one(&mut **tx).await
}
/// Held through semantic writes, so pause wins before persistence or waits
/// until the already committing window finishes. No evidence is erased.
pub async fn persistence_allowed_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    profile: &str,
) -> Result<bool, sqlx::Error> {
    let status:Option<String>=sqlx::query_scalar("SELECT r.status FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id AND u.graph_id=r.graph_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 AND u.extraction_version=$3 AND u.execution_owner FOR UPDATE OF r")
        .bind(graph).bind(version).bind(profile).fetch_optional(&mut **tx).await?;
    Ok(status.as_deref() == Some("running") && lifecycle_ready_tx(tx).await?)
}
/// A fresh authorization fence immediately before model dispatch and writes.
/// Reservations remain charged if a later revocation discards the response.
#[allow(clippy::too_many_arguments)]
pub async fn window_access_allowed_tx(
    tx: &mut Transaction<'_, Postgres>,
    control: &sqlx::PgPool,
    tenant: &crate::tenancy::TenantId,
    graph: Uuid,
    version: Uuid,
    profile: &str,
) -> Result<bool, crate::store::StoreError> {
    let active = persistence_allowed_tx(tx, graph, version, profile).await?;
    let requester = requester_allowed_tx(tx, control, tenant, graph, version, profile).await?;
    let source = source_permitted_tx(tx, graph, version, profile).await?;
    if !active || !requester || !source {
        hold_window_tx(
            tx,
            graph,
            version,
            profile,
            if !requester {
                "requester_access_lost"
            } else if !source {
                "source_removed_or_access_lost"
            } else {
                "managed_run_paused"
            },
        )
        .await?;
        return Ok(false);
    }
    Ok(true)
}
pub async fn window_outcome_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    profile: &str,
    state: &str,
    reason: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE corpus_capture_admissions SET state=$4,reason=$5,updated_at=now() WHERE graph_id=$1 AND source_version_id=$2 AND extraction_version=$3")
        .bind(graph).bind(version).bind(profile).bind(if state=="processed"{"processed"}else if state=="failed"{"failed"}else if state=="unavailable"{"unavailable"}else{"queued"}).bind(reason).execute(&mut **tx).await?;
    sqlx::query("UPDATE corpus_enrichment_units SET state=$4,reason=$5,updated_at=now() WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND extraction_version=$3 AND execution_owner")
        .bind(graph).bind(version).bind(profile).bind(state).bind(reason).execute(&mut **tx).await?;
    Ok(())
}

pub async fn requester_allowed_tx(
    tx: &mut Transaction<'_, Postgres>,
    control: &sqlx::PgPool,
    tenant: &crate::tenancy::TenantId,
    graph: Uuid,
    version: Uuid,
    profile: &str,
) -> Result<bool, crate::store::StoreError> {
    let actor:Option<(Uuid,Uuid)>=sqlx::query_as("SELECT r.requested_by,r.account_id FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id AND u.graph_id=r.graph_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 AND u.extraction_version=$3 AND u.execution_owner")
        .bind(graph).bind(version).bind(profile).fetch_optional(&mut **tx).await?;
    let Some((user, account)) = actor else {
        return Ok(false);
    };
    if matches!(tenant, crate::tenancy::TenantId::Consumer) {
        // Restore the caller's local scope even when requester and job scopes
        // differ; this is never a connection-global authority change.
        let prior: String = sqlx::query_scalar("SELECT COALESCE(current_setting('app.account_id',true),'')")
            .fetch_one(&mut **tx).await?;
        sqlx::query("SELECT set_config('app.account_id',$1,true)")
            .bind(account.to_string()).execute(&mut **tx).await?;
        let allowed = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2) AND resolve_graph_reachable_by_user($3,$2)",
        ).bind(account).bind(user).bind(graph).fetch_one(&mut **tx).await;
        sqlx::query("SELECT set_config('app.account_id',$1,true)")
            .bind(prior).execute(&mut **tx).await?;
        return Ok(allowed?);
    }
    let mut control_tx = crate::tenancy::scope::begin_account_scoped(control, account).await?;
    let current: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id=$1 AND user_id=$2)",
    )
    .bind(account)
    .bind(user)
    .fetch_one(&mut *control_tx)
    .await?;
    if !current {
        return Ok(false);
    }
    let allowed = match tenant {
        crate::tenancy::TenantId::Consumer => {
            sqlx::query_scalar("SELECT resolve_graph_reachable_by_user($1,$2)")
                .bind(graph)
                .bind(user)
                .fetch_one(&mut *control_tx)
                .await?
        }
        crate::tenancy::TenantId::Org(org) => {
            let revoked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM org_revocations WHERE clerk_org_id=$1) OR NOT EXISTS(SELECT 1 FROM accounts WHERE id=$2 AND clerk_org_id=$1)",
            )
            .bind(org).bind(account)
            .fetch_one(&mut *control_tx)
            .await?;
            if revoked {
                false
            } else {
                sqlx::query_scalar("SELECT resolve_tenant_graph_reachable_by_user($1,$2,$3)")
                    .bind(graph)
                    .bind(account)
                    .bind(user)
                    .fetch_one(&mut **tx)
                    .await?
            }
        }
    };
    control_tx.rollback().await?;
    Ok(allowed)
}

pub(crate) async fn resume_legacy_financial_holds_tx(
    conn: &mut sqlx::PgConnection, account: Uuid,
) -> Result<(),sqlx::Error> {
    sqlx::query("UPDATE corpus_enrichment_runs SET status='running',pause_reason=NULL,updated_at=now() \
      WHERE account_id=$1 AND run_kind='cohort' AND mode='execute' AND status='paused' \
      AND pause_reason='legacy_managed_liabilities_pending'")
      .bind(account).execute(conn).await?;
    Ok(())
}

pub async fn hold_window_tx(
    tx: &mut Transaction<'_, Postgres>,
    graph: Uuid,
    version: Uuid,
    profile: &str,
    reason: &str,
) -> Result<(), sqlx::Error> {
    let standing: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id WHERE u.graph_id=$1 AND u.holding_id=$2 AND u.extraction_version=$3 AND u.execution_owner AND r.run_kind='standing')")
        .bind(graph).bind(version).bind(profile).fetch_one(&mut **tx).await?;
    if standing {
        return standing::hold_unit_tx(tx, graph, version, profile, reason).await;
    }
    sqlx::query("UPDATE corpus_enrichment_runs r SET status='paused',pause_reason=CASE WHEN r.status='paused' THEN COALESCE(r.pause_reason,$4) ELSE $4 END,updated_at=now() FROM corpus_enrichment_units u WHERE u.run_id=r.run_id AND u.graph_id=r.graph_id AND u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 AND u.extraction_version=$3 AND u.execution_owner AND r.status<>'complete'")
        .bind(graph).bind(version).bind(profile).bind(reason).execute(&mut **tx).await?;
    sqlx::query("UPDATE corpus_enrichment_units u SET state='pending',reason=COALESCE(r.pause_reason,$4),updated_at=now() FROM corpus_enrichment_runs r WHERE u.run_id=r.run_id AND u.graph_id=r.graph_id AND u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 AND u.extraction_version=$3 AND u.execution_owner")
        .bind(graph).bind(version).bind(profile).bind(reason).execute(&mut **tx).await?;
    sqlx::query("UPDATE corpus_capture_admissions SET state='blocked',reason=$4,updated_at=now() WHERE graph_id=$1 AND source_version_id=$2 AND extraction_version=$3 AND state<>'processed'").bind(graph).bind(version).bind(profile).bind(reason).execute(&mut **tx).await?;
    sqlx::query("UPDATE source_versions SET extraction_status='blocked',extraction_error=COALESCE((SELECT r.pause_reason FROM corpus_enrichment_runs r JOIN corpus_enrichment_units u ON u.run_id=r.run_id WHERE u.graph_id=$1 AND COALESCE(u.promoted_version_id,u.holding_id)=$2 AND u.extraction_version=$3 AND u.execution_owner LIMIT 1),$4) WHERE graph_id=$1 AND version_id=$2 AND extraction_status<>'done' AND EXISTS(SELECT 1 FROM corpus_enrichment_units WHERE graph_id=$1 AND COALESCE(promoted_version_id,holding_id)=$2 AND extraction_version=$3 AND execution_owner)")
        .bind(graph).bind(version).bind(profile).bind(reason).execute(&mut **tx).await?;
    Ok(())
}

#[cfg(test)]
mod profile_tests {
    /// v1 removal, step 1: no new cohort run takes `source-atoms-v1`; every
    /// other profile still can.
    #[test]
    fn new_cohort_runs_refuse_the_retired_v1_profile() {
        assert!(!super::supported("source-atoms-v1"));
        for profile in ["source-atoms-v2", "source-atoms-v3", "meeting-atoms-v1"] {
            assert!(super::supported(profile), "{profile}");
        }
        assert!(!super::supported("source-atoms-v9"));
        assert_ne!(super::profile_default(), "source-atoms-v1", "the default is the active profile, never v1");
    }
}
