//! Explicit consent for exact retained evidence, never whole originals/answers.
//! Same content plane/account first; cross-plane delivery is a separate gate.
use crate::{
    library_read::LibraryJson, source_evidence, tenancy::extractor::Tenant, AppState, UserContext,
};
use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub passage_id: Uuid,
    pub start_char: i32,
    pub end_char: i32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishRequest {
    pub request_id: Uuid,
    pub source_graph_id: Uuid,
    pub source_item_id: Uuid,
    pub source_version_id: Uuid,
    pub target_graph_id: Uuid,
    pub passages: Vec<Selection>,
}
fn response(status: StatusCode, value: Value) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
}
fn fail(status: StatusCode, message: &str) -> Response {
    response(status, json!({"error":message}))
}
fn bad() -> Response {
    fail(StatusCode::BAD_REQUEST, "invalid selected evidence")
}
fn denied() -> Response {
    fail(StatusCode::NOT_FOUND, "evidence or destination unavailable")
}
fn internal() -> Response {
    fail(StatusCode::INTERNAL_SERVER_ERROR, "publication unavailable")
}

// Publications are owner-private on BOTH planes. The org scope already binds
// the tenant actor; the consumer member scope binds only the account, so the
// CONTROL policy's owner_user_id=app.user_id predicate needs the actor too.
pub(crate) async fn owner_tx(
    tenant: &Tenant,
    user: &UserContext,
) -> Result<Transaction<'static, Postgres>, crate::store::StoreError> {
    let mut tx = tenant.begin_member_scoped(user.account_id).await?;
    if tenant.org_context().is_none() {
        crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id).await?;
    }
    Ok(tx)
}
// Reuse the already acquired content connection for consumer CONTROL checks.
// Never hold a one-connection consumer TX then ask its own pool for admission.
async fn admitted_tx(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<bool, sqlx::Error> {
    let org = tenant.org_context().map(|c| c.org_id.as_str());
    async fn check(
        tx: &mut Transaction<'_, Postgres>,
        user: &UserContext,
        org: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let allowed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM account_members m JOIN accounts a ON a.id=m.account_id WHERE m.account_id=$1 AND m.user_id=$2 AND ($3::text IS NULL OR a.clerk_org_id=$3) AND NOT EXISTS(SELECT 1 FROM org_revocations WHERE clerk_org_id=$3) AND COALESCE(org_deletion_state_for_org($3),'') NOT IN ('requested','grace','awaiting_approval','destroying'))")
            .bind(user.account_id).bind(user.user_id).bind(org).fetch_one(&mut **tx).await?;
        Ok(allowed)
    }
    if org.is_none() {
        return check(tx, user, None).await;
    }
    let Some(pool) = state.control_db() else {
        return Ok(false);
    };
    let mut control = crate::tenancy::scope::begin_account_scoped(pool, user.account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let allowed = check(&mut control, user, org).await?;
    control.commit().await?;
    Ok(allowed)
}
async fn graphs_tx(
    tenant: &Tenant,
    user: &UserContext,
    tx: &mut Transaction<'_, Postgres>,
    source: Uuid,
    target: Uuid,
) -> Result<bool, sqlx::Error> {
    let reach = if tenant.org_context().is_some() {
        "resolve_tenant_graph_reachable_by_user(g.id,$3,$4)"
    } else {
        "resolve_graph_reachable_by_user(g.id,$4)"
    };
    let sql=format!("SELECT count(*)=2 FROM graphs g WHERE g.id=ANY(ARRAY[$1,$2]::uuid[]) AND g.account_id=$3 AND g.archived_at IS NULL AND COALESCE({reach},false) AND ((g.id=$1 AND ((g.kind='member' AND g.owner_user_id=$4) OR g.kind='personal')) OR (g.id=$2 AND g.kind IN('team','org')))");
    sqlx::query_scalar(&sql)
        .bind(source)
        .bind(target)
        .bind(user.account_id)
        .bind(user.user_id)
        .fetch_one(&mut **tx)
        .await
}

async fn active_for_emission(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
    source: Uuid,
    target: Uuid,
    id: Uuid,
) -> bool {
    for graph in [source, target] {
        if !source_evidence::still_admitted(state, tenant, user, graph, user.account_id).await {
            return false;
        }
    }
    let Ok(mut tx) = owner_tx(&tenant, &user).await else {
        return false;
    };
    let allowed = matches!(
        admitted_tx(state, tenant, user, &mut tx).await,
        Ok(true)
    ) && matches!(
        graphs_tx(tenant, user, &mut tx, source, target).await,
        Ok(true)
    );
    let active = if allowed {
        sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM selected_evidence_publications p JOIN source_items s ON s.graph_id=p.source_graph_id AND s.source_item_id=p.source_item_id JOIN source_items t ON t.graph_id=p.target_graph_id AND t.source_item_id=p.target_source_item_id JOIN source_versions v ON v.graph_id=t.graph_id AND v.source_item_id=t.source_item_id AND v.version_id=p.target_source_version_id WHERE p.publication_id=$1 AND p.owner_user_id=$2 AND p.status='active' AND s.access_state='permitted' AND t.access_state='permitted' AND v.source_metadata->>'publication_receipt_id'=p.publication_id::text)").bind(id).bind(user.user_id).fetch_one(&mut *tx).await.unwrap_or(false)
    } else {
        false
    };
    tx.commit().await.is_ok() && active
}

pub async fn publish(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(mut q): LibraryJson<PublishRequest>,
) -> Response {
    if q.source_graph_id == q.target_graph_id || q.passages.is_empty() || q.passages.len() > 32 {
        return bad();
    }
    q.passages
        .sort_by_key(|p| (p.start_char, p.end_char, p.passage_id));
    let mut total = 0i64;
    let mut seen = std::collections::HashSet::new();
    for p in &q.passages {
        if p.start_char < 0
            || p.end_char <= p.start_char
            || p.end_char - p.start_char > 4000
            || !seen.insert(p.passage_id)
        {
            return bad();
        }
        total += i64::from(p.end_char - p.start_char);
    }
    if total > 64000 {
        return bad();
    }
    for graph in [q.source_graph_id, q.target_graph_id] {
        match source_evidence::authorize(&tenant, &user, graph).await {
            Ok(a) if a == user.account_id => {}
            Ok(_) => {
                return fail(
                    StatusCode::CONFLICT,
                    "cross-account publication is not available",
                )
            }
            Err(_) => return denied(),
        }
        if !source_evidence::still_admitted(&state, &tenant, &user, graph, user.account_id).await {
            return denied();
        }
    }
    let mut tx = match owner_tx(&tenant, &user).await {
        Ok(tx) => tx,
        Err(_) => return internal(),
    };
    if !matches!(
        graphs_tx(
            &tenant,
            &user,
            &mut tx,
            q.source_graph_id,
            q.target_graph_id
        )
        .await,
        Ok(true)
    ) {
        return denied();
    }
    let row =
        match source_evidence::load_version_scoped(&mut tx, q.source_graph_id, q.source_version_id)
            .await
        {
            Ok(Some(r)) if r.source_item_id == q.source_item_id => r,
            _ => return denied(),
        };
    if row.representation == "generated_summary" {
        return fail(StatusCode::CONFLICT,"Select original or provider-retained evidence; generated summaries lack complete publication inputs");
    }
    let existing:Option<(Uuid,Uuid,Uuid,Uuid,Uuid,Value,String)>=match sqlx::query_as("SELECT publication_id,source_graph_id,source_item_id,source_version_id,target_graph_id,selection,status FROM selected_evidence_publications WHERE account_id=$1 AND owner_user_id=$2 AND request_id=$3").bind(user.account_id).bind(user.user_id).bind(q.request_id).fetch_optional(&mut *tx).await{Ok(r)=>r,Err(_)=>return internal()};
    if let Some((id, g, i, v, t, selection, status)) = existing {
        if (g, i, v, t)
            != (
                q.source_graph_id,
                q.source_item_id,
                q.source_version_id,
                q.target_graph_id,
            )
            || selection != json!(q.passages)
        {
            return fail(StatusCode::CONFLICT, "request ID has different consent");
        }
        if status != "active" {
            return fail(StatusCode::CONFLICT, "consent has been retracted");
        }
        let receipt:Value=match sqlx::query_scalar("SELECT jsonb_build_object('publication_id',publication_id,'graph_id',target_graph_id,'source_item_id',target_source_item_id,'source_version_id',target_source_version_id,'processing_status','deterministic_selected_passages','managed_consumers_status','pending_explicit_budget') FROM selected_evidence_publications WHERE publication_id=$1").bind(id).fetch_one(&mut *tx).await{Ok(v)=>v,Err(_)=>return internal()};
        if !matches!(
            admitted_tx(&state, &tenant, &user, &mut tx).await,
            Ok(true)
        ) {
            return denied();
        }
        if tx.commit().await.is_err() {
            return internal();
        }
        if !active_for_emission(
            &state,
            &tenant,
            &user,
            q.source_graph_id,
            q.target_graph_id,
            id,
        )
        .await
        {
            return denied();
        }
        return response(StatusCode::OK, receipt);
    }
    // Release the transaction before R2/decryption/CONTROL waits.
    if tx.commit().await.is_err() {
        return internal();
    }
    if let Some(key) = row.raw_blob_key.as_deref() {
        let parts: Vec<_> = key.split('/').collect();
        let graph = row.graph_id.to_string();
        let owns = match (tenant.org_context(), parts.as_slice()) {
            (None, [g, _]) => *g == graph,
            (Some(org), [o, g, _]) => *o == org.org_id && *g == graph,
            _ => false,
        };
        if !owns || coralswarm_core::r2::validate_blob_key(key).is_err() {
            return denied();
        }
    }
    let Some(text) = source_evidence::read_text(&state, &tenant, user.account_id, &row).await
    else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "retained original is unavailable",
        );
    };
    if text.len() > 8 * 1024 * 1024 {
        return fail(
            StatusCode::PAYLOAD_TOO_LARGE,
            "retained original exceeds publication verification limit",
        );
    }
    let hash = crate::services::source_reingest::content_hash_hex(&text);
    if row.content_sha256.as_ref().is_some_and(|h| h != &hash) {
        return denied();
    }
    for graph in [q.source_graph_id, q.target_graph_id] {
        if !source_evidence::still_admitted(&state, &tenant, &user, graph, user.account_id).await {
            return denied();
        }
    }
    let chars: Vec<char> = text.chars().collect();
    let mut tx = match owner_tx(&tenant, &user).await {
        Ok(tx) => tx,
        Err(_) => return internal(),
    };
    // Locks bind bytes and permission to this exact immutable version. Verify
    // locator/inline identity too: a SQL rewrite cannot certify fetched old bytes.
    if sqlx::query("SELECT s.source_item_id FROM source_items s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id WHERE s.graph_id=$1 AND s.source_item_id=$2 AND v.version_id=$3 FOR UPDATE OF s,v").bind(q.source_graph_id).bind(q.source_item_id).bind(q.source_version_id).fetch_optional(&mut *tx).await.ok().flatten().is_none(){return denied()}
    let stable:bool=match sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM source_versions v JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id WHERE v.graph_id=$1 AND v.version_id=$2 AND v.source_item_id=$3 AND s.access_state='permitted' AND v.raw_inline IS NOT DISTINCT FROM $4 AND v.raw_blob_key IS NOT DISTINCT FROM $5 AND v.content_sha256 IS NOT DISTINCT FROM $6)").bind(q.source_graph_id).bind(q.source_version_id).bind(q.source_item_id).bind(&row.raw_inline).bind(&row.raw_blob_key).bind(&row.content_sha256).fetch_one(&mut *tx).await{Ok(b)=>b,Err(_)=>return internal()};
    if !stable {
        return denied();
    }
    if !matches!(
        graphs_tx(
            &tenant,
            &user,
            &mut tx,
            q.source_graph_id,
            q.target_graph_id
        )
        .await,
        Ok(true)
    ) || !matches!(
        admitted_tx(&state, &tenant, &user, &mut tx).await,
        Ok(true)
    ) {
        return denied();
    }
    let mut quotes = Vec::new();
    for p in &q.passages {
        let passage:Option<(i32,i32,String)>=match sqlx::query_as("SELECT p.start_char,p.end_char,p.quote FROM semantic_atom_passages p JOIN semantic_atoms a ON a.graph_id=p.graph_id AND a.source_version_id=p.source_version_id AND a.semantic_atom_id=p.semantic_atom_id WHERE p.graph_id=$1 AND p.source_version_id=$2 AND p.passage_id=$3 FOR SHARE OF p,a").bind(q.source_graph_id).bind(q.source_version_id).bind(p.passage_id).fetch_optional(&mut *tx).await{Ok(r)=>r,Err(_)=>return internal()};
        let Some((start, end, quote)) = passage else {
            return denied();
        };
        if p.start_char < start
            || p.end_char > end
            || end as usize > chars.len()
            || start < 0
            || end <= start
            || chars[start as usize..end as usize]
                .iter()
                .collect::<String>()
                != quote
        {
            return bad();
        }
        quotes.push(
            chars[p.start_char as usize..p.end_char as usize]
                .iter()
                .collect::<String>(),
        );
    }
    // Serialize idempotent consent before any destination row is created.
    if sqlx::query("SELECT pg_advisory_xact_lock(hashtext('selected_publication:'||$1))")
        .bind(format!(
            "{}:{}:{}",
            user.account_id, user.user_id, q.request_id
        ))
        .execute(&mut *tx)
        .await
        .is_err()
    {
        return internal();
    }
    let raced:bool=match sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM selected_evidence_publications WHERE account_id=$1 AND owner_user_id=$2 AND request_id=$3)").bind(user.account_id).bind(user.user_id).bind(q.request_id).fetch_one(&mut *tx).await{Ok(b)=>b,Err(_)=>return internal()};
    if raced {
        return fail(
            StatusCode::CONFLICT,
            "consent already recorded; retry the request",
        );
    }
    let mut origin = crate::source_origin::SourceOrigin::native(
        "coralswarm",
        "evidence_excerpt",
        "selected_publication",
    )
    .expect("static selected origin");
    if row.origin.as_ref().is_some_and(|o| {
        matches!(
            o.transport,
            crate::source_origin::CaptureTransport::Synthetic
        )
    }) {
        origin.transport = crate::source_origin::CaptureTransport::Synthetic;
    }
    let origin = json!(origin);
    let publication = Uuid::new_v4();
    let item = Uuid::new_v4();
    let version = Uuid::new_v4();
    let selected = quotes.join("\n\n");
    let selected_hash = crate::services::source_reingest::content_hash_hex(&selected);
    let metadata = json!({"synthetic_demo":row.origin.as_ref().is_some_and(|o|matches!(o.transport,crate::source_origin::CaptureTransport::Synthetic)),"title":"Selected evidence","publication_receipt_id":publication,"independence":"derived_copy","validity_status":"unknown","review_status":"user_selected","disagreement_status":"unresolved"});
    let source_id = publication.to_string();
    let namespace = format!("selected:{}", q.target_graph_id);
    let result=async{
        sqlx::query("INSERT INTO source_items(source_item_id,graph_id,source_type,provider_namespace,source_id,origin) VALUES($1,$2,'selected_evidence',$3,$4,$5)").bind(item).bind(q.target_graph_id).bind(&namespace).bind(&source_id).bind(&origin).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO source_versions(version_id,graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,fingerprint_sha256,representation,source_metadata,ingested_by,scope,extraction_version,extraction_status,next_start_char,origin) VALUES($1,$2,$3,$4,'selected_evidence',$5,$6,$7,$7,$12,$8,$9,'shared','selected-passages-v1','done',$10,$11)").bind(version).bind(q.target_graph_id).bind(item).bind(&source_id).bind(&namespace).bind(&selected).bind(&selected_hash).bind(&metadata).bind(user.user_id).bind(selected.chars().count() as i64).bind(&origin).bind(&row.representation).execute(&mut *tx).await?;
        sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1").bind(item).bind(version).execute(&mut *tx).await?;
        let mut offset=0i32;
        for (ordinal,quote) in quotes.iter().enumerate(){
            let atom=Uuid::new_v4();let end=offset+quote.chars().count() as i32;
            sqlx::query("INSERT INTO semantic_atoms(semantic_atom_id,graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,$3,$4,'observation',$5,'selected-passages-v1','deterministic-selection')").bind(atom).bind(q.target_graph_id).bind(version).bind(ordinal as i32).bind(quote).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO semantic_atom_passages(semantic_atom_id,graph_id,source_version_id,start_char,end_char,quote) VALUES($1,$2,$3,$4,$5,$6)").bind(atom).bind(q.target_graph_id).bind(version).bind(offset).bind(end).bind(quote).execute(&mut *tx).await?;offset=end+2;
        }
        sqlx::query("INSERT INTO selected_evidence_publications(publication_id,account_id,owner_user_id,request_id,source_graph_id,source_item_id,source_version_id,selection,passage_ids,target_graph_id,target_source_item_id,target_source_version_id,selected_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)").bind(publication).bind(user.account_id).bind(user.user_id).bind(q.request_id).bind(q.source_graph_id).bind(q.source_item_id).bind(q.source_version_id).bind(json!(q.passages)).bind(q.passages.iter().map(|p|p.passage_id).collect::<Vec<_>>()).bind(q.target_graph_id).bind(item).bind(version).bind(&selected_hash).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO corpus_capture_admissions(graph_id,account_id,source_item_id,source_version_id,captured_by,extraction_version,state,reason) VALUES($1,$2,$3,$4,$5,'selected-passages-v1','processed','deterministic_selected_passages_no_model')").bind(q.target_graph_id).bind(user.account_id).bind(item).bind(version).bind(user.user_id).execute(&mut *tx).await?;
        Ok::<_,sqlx::Error>(())
    }.await;
    if result.is_err() {
        return internal();
    }
    if tx.commit().await.is_err() {
        return internal();
    }
    if !active_for_emission(
        &state,
        &tenant,
        &user,
        q.source_graph_id,
        q.target_graph_id,
        publication,
    )
    .await
    {
        return denied();
    }
    response(
        StatusCode::CREATED,
        json!({"publication_id":publication,"graph_id":q.target_graph_id,"source_item_id":item,"source_version_id":version,"processing_status":"deterministic_selected_passages","managed_consumers_status":"pending_explicit_budget"}),
    )
}

pub async fn retract(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let mut tx = match owner_tx(&tenant, &user).await {
        Ok(tx) => tx,
        Err(_) => return internal(),
    };
    let record:Option<(Uuid,Uuid,Uuid,String)>=match sqlx::query_as("SELECT source_graph_id,target_graph_id,target_source_item_id,status FROM selected_evidence_publications WHERE publication_id=$1 AND owner_user_id=$2 FOR UPDATE").bind(id).bind(user.user_id).fetch_optional(&mut *tx).await{Ok(r)=>r,Err(_)=>return internal()};
    let Some((source, target, item, status)) = record else {
        return denied();
    };
    if !matches!(
        graphs_tx(&tenant, &user, &mut tx, source, target).await,
        Ok(true)
    ) || !matches!(
        admitted_tx(&state, &tenant, &user, &mut tx).await,
        Ok(true)
    ) {
        return denied();
    }
    if status == "active" {
        if sqlx::query("DELETE FROM source_items s WHERE s.graph_id=$1 AND s.source_item_id=$2 AND EXISTS(SELECT 1 FROM source_versions v WHERE v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.source_metadata->>'publication_receipt_id'=$3)").bind(target).bind(item).bind(id.to_string()).execute(&mut *tx).await.is_err(){return internal()}
        if sqlx::query("UPDATE selected_evidence_publications SET status='retracted',retracted_at=now() WHERE publication_id=$1").bind(id).execute(&mut *tx).await.is_err(){return internal()}
    }
    if tx.commit().await.is_err() {
        return internal();
    }
    response(
        StatusCode::OK,
        json!({"publication_id":id,"status":"retracted"}),
    )
}

/// Legacy whole-object/standing consent cannot attest selected retained bytes.
/// The typed publication API is the only cloud publication writer until the
/// versioned standing-consent follow-on is reviewed.
pub(crate) fn legacy_whole_source_authorized() -> bool {
    false
}
