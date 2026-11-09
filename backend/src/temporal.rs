//! Recorded historical state: explicit validity, immutable knowledge and named
//! transitions. Permission and lifecycle today always constrain past replay.
use crate::{library_read::{self, LibraryScope, OutcomeVersions}, middleware::auth::UserContext, tenancy::extractor::Tenant, AppState};
use axum::{extract::State, Extension, http::{header, StatusCode}, response::{IntoResponse, Response}, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use std::{collections::{HashMap, HashSet, BTreeMap}, sync::Arc};
use uuid::Uuid;

pub const RECORD_LIMIT: usize = 128;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(tag="mode", rename_all="snake_case", deny_unknown_fields)]
pub enum TemporalSelection {
    Ordinary { revision: u8 },
    Historical { revision: u8, state_key: String, effective_at: DateTime<Utc>, as_known_at: DateTime<Utc> },
}
impl Default for TemporalSelection { fn default() -> Self { Self::Ordinary { revision: 1 } } }
impl TemporalSelection {
    pub fn is_historical(&self) -> bool { matches!(self, Self::Historical { .. }) }
    pub fn validate(&self, scope: &LibraryScope) -> Result<(), &'static str> {
        let revision = match self { Self::Ordinary{revision} | Self::Historical{revision,..} => *revision };
        if revision != 1 { return Err("unsupported temporal revision"); }
        if let Self::Historical{state_key,as_known_at,..}=self {
            if !valid_key(state_key) || scope.node_id.is_none() { return Err("historical state requires a canonical node and state key"); }
            if *as_known_at > Utc::now() { return Err("future knowledge cannot be reconstructed"); }
            if scope.outcome_versions == Some(OutcomeVersions::Current) { return Err("historical state requires retained evidence"); }
        }
        Ok(())
    }
}
fn valid_key(key: &str) -> bool {
    let bytes=key.as_bytes();
    (1..=64).contains(&bytes.len()) && bytes[0].is_ascii_lowercase() && bytes.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"_.-".contains(c))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRequest {
    pub scope: LibraryScope,
    pub state_key: String,
    pub semantic_atom_id: Uuid,
    pub passage_id: Uuid,
    pub operation: String,
    pub value: Option<String>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub validity_quote: Option<String>,
    pub predecessor_event_id: Option<Uuid>,
}

#[derive(Clone, sqlx::FromRow)]
pub(crate) struct Record {
    pub event_id: Uuid, pub graph_id: Uuid, pub subject_node_id: Uuid,
    pub state_key: String, pub source_item_id: Uuid, pub source_version_id: Uuid,
    pub semantic_atom_id: Uuid, pub passage_id: Uuid, pub known_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>, pub operation: String, pub value: Option<String>,
    pub valid_from: Option<DateTime<Utc>>, pub valid_until: Option<DateTime<Utc>>,
    pub predecessor_event_id: Option<Uuid>, pub recording_method: String,
    pub quote: String, pub start_char: i64, pub end_char: i64,
    pub statement: String, pub kind: String, pub representation: String,
    pub captured_at: DateTime<Utc>, pub occurred_at: Option<DateTime<Utc>>,
    pub source_version_is_current: bool, pub synthetic_demo: bool, pub confidence: Option<f64>,
}
impl Record {
    fn contains(&self, at: DateTime<Utc>) -> bool {
        self.valid_from.is_some_and(|start|start<=at) && self.valid_until.is_none_or(|end|at<end)
    }
    fn citation(&self) -> Value {
        json!({"kind":"source_passage","graph_id":self.graph_id,"source_item_id":self.source_item_id,
            "source_version_id":self.source_version_id,"semantic_atom_id":self.semantic_atom_id,"passage_id":self.passage_id,
            "start_char":self.start_char,"end_char":self.end_char.min(self.start_char+4000),
            "quote":self.quote.chars().take(4000).collect::<String>(),"quote_truncated":self.quote.chars().count()>4000,
            "representation":self.representation,"occurred_at":self.occurred_at,"captured_at":self.captured_at})
    }
    fn outcome(&self) -> Value {
        json!({"graph_id":self.graph_id,"semantic_atom_id":self.semantic_atom_id,"source_item_id":self.source_item_id,
            "source_version_id":self.source_version_id,"source_version_is_current":self.source_version_is_current,
            "kind":self.kind,"statement":self.statement,"statement_truncated":false,"confidence":self.confidence,
            "occurred_at":self.occurred_at,"captured_at":self.captured_at,"representation":self.representation,
            "synthetic_demo":self.synthetic_demo,"citations":[self.citation()]})
    }
}

pub(crate) struct Reconstruction {
    pub state: Value,
    pub records: Vec<Record>,
    pub truncated: bool,
}
impl Reconstruction {
    /// Every input, including superseded/control/ancestor evidence, enters the
    /// existing source_passage receipt path; model-selected citations are not authority.
    pub fn candidates(&self, scope: &LibraryScope) -> Value {
        let mut seen=HashSet::new();
        let outcomes:Vec<Value>=self.records.iter().filter(|r|seen.insert((r.semantic_atom_id,r.passage_id))).map(Record::outcome).collect();
        json!({"scope":scope,"outcomes":outcomes,"legacy_evidence":[],"truncated":self.truncated,
            "reason":"recorded_temporal_state","temporal_state":self.state})
    }
    pub fn event_ids(&self) -> Vec<Uuid> { self.records.iter().map(|r|r.event_id).collect() }
    pub fn support(&self) -> Value {
        json!(self.records.iter().map(|r|json!({"kind":"source_passage","graph_id":r.graph_id,"source_item_id":r.source_item_id,
            "source_version_id":r.source_version_id,"semantic_atom_id":r.semantic_atom_id,"passage_id":r.passage_id})).collect::<Vec<_>>())
    }
}

/// Fold correction links first, then explicit effective transitions. Parallel
/// heads are retained; neither capture nor knowledge order chooses a winner.
fn reconstruct(records: &[Record], selection: &TemporalSelection, truncated: bool) -> Value {
    let TemporalSelection::Historical{effective_at,..}=selection else { return Value::Null };
    let by_id:HashMap<Uuid,&Record>=records.iter().map(|r|(r.event_id,r)).collect();
    let corrected:HashSet<Uuid>=records.iter().filter(|r|r.operation=="correct").filter_map(|r|r.predecessor_event_id).collect();
    // Each head carries its whole correction chain: a transition/withdrawal may
    // name the original decision or any correction of it.
    let mut heads=Vec::new(); let mut chains=Vec::new(); let mut incomplete=truncated;
    for r in records.iter().filter(|r|!corrected.contains(&r.event_id)) {
        let mut base=r; let mut visited=HashSet::from([r.event_id]);
        while base.operation=="correct" {
            let Some(parent)=base.predecessor_event_id.and_then(|id|by_id.get(&id).copied()) else { incomplete=true; break };
            if !visited.insert(parent.event_id) || visited.len()>RECORD_LIMIT { incomplete=true; break; }
            base=parent;
        }
        heads.push((r,base)); chains.push(visited);
    }
    let mut active=BTreeMap::<String,Vec<&Record>>::new();
    let mut bases=HashSet::new();
    let parallel_corrections=heads.iter().any(|(_,base)|!bases.insert(base.event_id));
    if heads.iter().any(|(_,base)|base.predecessor_event_id.is_some_and(|id|!by_id.contains_key(&id))) { incomplete=true; }
    for ((r,base),chain) in heads.iter().zip(&chains) {
        if base.operation=="withdraw" { continue; }
        if r.valid_from.is_none() || r.value.is_none() { incomplete=true; continue; }
        if !r.contains(*effective_at) { continue; }
        // A transition names the particular prior decision (including a
        // corrected version of that same decision), not every equal value.
        let ended=heads.iter().any(|(control,original)| {
            matches!(original.operation.as_str(),"transition"|"withdraw") && original.predecessor_event_id.is_some_and(|id|chain.contains(&id))
                && control.valid_from.is_some_and(|start|start<=*effective_at)
        });
        if matches!(base.operation.as_str(),"transition"|"withdraw") && base.predecessor_event_id.is_some_and(|id|!by_id.contains_key(&id)) { incomplete=true; }
        if !ended { if let Some(value)=&r.value { active.entry(value.clone()).or_default().push(r); } }
    }
    // Unknown transition/withdrawal bounds cannot certify the retained prior state.
    if heads.iter().any(|(r,base)|matches!(base.operation.as_str(),"transition"|"withdraw") && r.valid_from.is_none()) { incomplete=true; }
    let values:Vec<Value>=active.into_iter().map(|(value,rs)| {
        let sources:HashSet<Uuid>=rs.iter().map(|r|r.source_item_id).collect();
        json!({"value":value,"source_count":sources.len(),"event_ids":rs.iter().map(|r|r.event_id).collect::<Vec<_>>(),"citations":rs.iter().map(|r|r.citation()).collect::<Vec<_>>()})
    }).collect();
    let status=if incomplete {"incomplete"} else if records.is_empty() {"unsupported"} else if parallel_corrections || values.len()>1 {"disputed"} else if values.is_empty() {"unknown"} else {"supported"};
    let controls:Vec<Value>=records.iter().map(|r|json!({"event_id":r.event_id,"operation":r.operation,"value":r.value,"semantic_kind":r.kind,"interpretation":"curated_declared_validity","epistemic_status":if r.recording_method=="human_curated_v1" {"human_interpretation"}else{"recorded_interpretation"},
        "valid_from":r.valid_from,"valid_until":r.valid_until,"known_at":r.known_at,"evidence_observed_at":r.observed_at,
        "recording_method":r.recording_method,"predecessor_event_id":r.predecessor_event_id.filter(|id|by_id.contains_key(id)),
        "relation_status":if r.predecessor_event_id.is_none() {"not_applicable"} else if r.predecessor_event_id.is_some_and(|id|by_id.contains_key(&id)) {"recorded_link"} else {"unavailable"},
        "citation":r.citation()})).collect();
    json!({"mode":"recorded_evidence_state","temporal":selection,"status":status,"history_coverage":"explicit_records_only","state_basis":"declared_interpretations_not_verified_provider_state",
        "values":values,"records":controls,"truncated":truncated,"parallel_corrections":parallel_corrections,"contradictions_resolved":false})
}

pub(crate) async fn read_tx(tx: &mut Transaction<'_,Postgres>, scope: &LibraryScope, selection: &TemporalSelection) -> Result<Reconstruction,sqlx::Error> {
    let TemporalSelection::Historical{state_key,as_known_at,..}=selection else {
        return Ok(Reconstruction{state:Value::Null,records:vec![],truncated:false})
    };
    let sql=format!(r#"{} SELECT e.event_id,e.graph_id,e.subject_node_id,e.state_key,e.source_item_id,e.source_version_id,e.semantic_atom_id,e.passage_id,e.known_at,COALESCE((SELECT min(o2.known_at) FROM source_version_observations o2 WHERE o2.graph_id=e.graph_id AND o2.source_version_id=e.source_version_id AND o2.known_at<=e.known_at),e.known_at) observed_at,
        e.operation,e.value,e.valid_from,e.valid_until,e.predecessor_event_id,e.recording_method,p.quote,p.start_char::bigint start_char,p.end_char::bigint end_char,
        a.statement,a.kind,a.representation,a.captured_at,a.occurred_at,a.source_version_is_current,
        (COALESCE(a.origin->>'transport'='synthetic',false) OR COALESCE(a.source_metadata->'synthetic_demo'='true'::jsonb,false)) synthetic_demo,a.confidence::double precision confidence
        FROM scope_atoms a JOIN temporal_evidence_records e ON e.graph_id=a.graph_id AND e.source_version_id=a.source_version_id AND e.source_item_id=a.source_item_id AND e.semantic_atom_id=a.semantic_atom_id
        JOIN semantic_atom_passages p ON p.passage_id=e.passage_id AND p.graph_id=e.graph_id AND p.semantic_atom_id=e.semantic_atom_id AND p.source_version_id=e.source_version_id
        JOIN node_semantic_atoms l ON l.graph_id=e.graph_id AND l.node_id=e.subject_node_id AND l.semantic_atom_id=e.semantic_atom_id
        WHERE e.subject_node_id=$5 AND e.state_key=$7 AND e.known_at<=$8 AND e.lifecycle='supported' AND library_node_permitted(e.graph_id,e.subject_node_id)
        ORDER BY e.known_at,e.event_id LIMIT 129"#,library_read::scope_cte(scope));
    let mut records:Vec<Record>=sqlx::query_as(&sql).bind(&scope.graph_ids).bind(scope.origin_filter()).bind(scope.source_item_id)
        .bind(scope.source_version_id).bind(scope.node_id).bind(scope.collection_key()).bind(state_key).bind(as_known_at).fetch_all(&mut **tx).await?;
    let truncated=records.len()>RECORD_LIMIT; records.truncate(RECORD_LIMIT);
    // Item/version/origin/collection selectors may exclude readable records for
    // the same node and state (e.g. the transition lives in another source).
    // Never present such a slice as confident state: it is incomplete. Only
    // records this reader may already see (RLS + node permission) are counted.
    let readable:i64=sqlx::query_scalar("SELECT count(*) FROM temporal_evidence_records e
        JOIN node_semantic_atoms l ON l.graph_id=e.graph_id AND l.node_id=e.subject_node_id AND l.semantic_atom_id=e.semantic_atom_id
        WHERE e.graph_id=ANY($1) AND e.subject_node_id=$2 AND e.state_key=$3 AND e.known_at<=$4 AND e.lifecycle='supported' AND library_node_permitted(e.graph_id,e.subject_node_id)")
        .bind(&scope.graph_ids).bind(scope.node_id).bind(state_key).bind(as_known_at).fetch_one(&mut **tx).await?;
    let scope_limited=!truncated && readable>records.len() as i64;
    let mut state=reconstruct(&records,selection,truncated||scope_limited);
    state["coverage_scope"]=json!(if scope_limited {"scope_limited"} else {"selected_scope"});
    Ok(Reconstruction{state,records,truncated})
}

pub(crate) async fn receipt_tx(tx:&mut Transaction<'_,Postgres>,receipt:Uuid,graph:Uuid,user:Uuid,selection:&TemporalSelection,reconstruction:&Reconstruction)->Result<(),sqlx::Error> {
    sqlx::query("INSERT INTO answer_temporal_receipts(receipt_id,graph_id,owner_user_id,temporal_selection,event_ids) VALUES($1,$2,$3,$4,$5)")
        .bind(receipt).bind(graph).bind(user).bind(json!(selection)).bind(reconstruction.event_ids()).execute(&mut **tx).await?;
    Ok(())
}

fn validate_record(q:&RecordRequest)->Result<(),&'static str> {
    if !valid_key(&q.state_key) || q.scope.node_id.is_none() || q.scope.source_item_id.is_none() || q.scope.source_version_id.is_none()
        || !matches!(q.operation.as_str(),"assert"|"transition"|"correct"|"withdraw")
        || (q.operation=="assert")!=q.predecessor_event_id.is_none()
        // A correction's value nullity follows the operation it corrects; the
        // database trigger checks that against the recorded chain.
        || (q.operation=="withdraw" && q.value.is_some())
        || (matches!(q.operation.as_str(),"assert"|"transition") && q.value.is_none())
        || q.value.as_ref().is_some_and(|v|v.trim().is_empty() || v.chars().count()>512 || v.contains('\0'))
        || q.valid_until.is_some_and(|end|q.valid_from.is_none_or(|start|end<=start)) {
        return Err("invalid temporal evidence declaration")
    }
    if q.valid_from.is_some() && q.validity_quote.as_ref().is_none_or(|q|q.trim().is_empty() || q.chars().count()>4000) {
        return Err("declared validity requires its exact dated quote")
    }
    for date in q.valid_from.iter().chain(q.valid_until.iter()) {
        if !q.validity_quote.as_ref().is_some_and(|quote|quote.contains(&date.date_naive().to_string())) {
            return Err("validity requires explicit ISO date evidence; uncertain dates remain unknown")
        }
    }
    Ok(())
}

/// The same writer validates both curation and the separately versioned managed
/// proposal contract. Method is server supplied; public requests cannot claim
/// provider-reported history or worker identity.
pub(crate) async fn record_tx(tx:&mut Transaction<'_,Postgres>,user:Uuid,q:&RecordRequest,method:&str)->Result<Value,String> {
    validate_record(q).map_err(str::to_string)?;
    if !matches!(method,"human_curated_v1"|"temporal_extraction_v1") {return Err("unsupported temporal extraction contract".into())}
    if !library_read::validate_objects_tx(tx,&q.scope).await.map_err(|_|"temporal scope unavailable")? {return Err("temporal scope unavailable".into())}
    let sql=format!(r#"{} SELECT a.graph_id,p.quote FROM scope_atoms a JOIN semantic_atom_passages p ON p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id AND p.semantic_atom_id=a.semantic_atom_id
        WHERE a.semantic_atom_id=$7 AND p.passage_id=$8 AND EXISTS(SELECT 1 FROM scope_nodes n WHERE n.graph_id=a.graph_id AND n.node_id=$5)"#,library_read::scope_cte(&q.scope));
    let (graph,quote):(Uuid,String)=sqlx::query_as(&sql).bind(&q.scope.graph_ids).bind(q.scope.origin_filter()).bind(q.scope.source_item_id)
        .bind(q.scope.source_version_id).bind(q.scope.node_id).bind(q.scope.collection_key()).bind(q.semantic_atom_id).bind(q.passage_id)
        .fetch_optional(&mut **tx).await.map_err(|_|"temporal evidence unavailable")?.ok_or("temporal evidence unavailable")?;
    if q.validity_quote.as_ref().is_some_and(|anchor|!quote.contains(anchor)) || q.value.as_ref().is_some_and(|value|!quote.to_lowercase().contains(&value.to_lowercase())) {
        return Err("temporal value and validity must identify the retained passage".into())
    }
    let observation:Uuid=sqlx::query_scalar("INSERT INTO source_version_observations(graph_id,source_item_id,source_version_id,observation_kind) VALUES($1,$2,$3,'annotation_recorded') RETURNING observation_id")
        .bind(graph).bind(q.scope.source_item_id).bind(q.scope.source_version_id).fetch_one(&mut **tx).await.map_err(|_|"temporal observation unavailable")?;
    let (event,known):(Uuid,DateTime<Utc>)=sqlx::query_as("INSERT INTO temporal_evidence_records(graph_id,subject_node_id,state_key,source_item_id,source_version_id,semantic_atom_id,passage_id,observation_id,operation,value,valid_from,valid_until,validity_quote,predecessor_event_id,recording_method,recorded_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING event_id,known_at")
        .bind(graph).bind(q.scope.node_id).bind(&q.state_key).bind(q.scope.source_item_id).bind(q.scope.source_version_id).bind(q.semantic_atom_id).bind(q.passage_id)
        .bind(observation).bind(&q.operation).bind(&q.value).bind(q.valid_from).bind(q.valid_until).bind(&q.validity_quote).bind(q.predecessor_event_id).bind(method).bind(user)
        .fetch_one(&mut **tx).await.map_err(|_|"temporal predecessor or evidence unavailable")?;
    Ok(json!({"event_id":event,"known_at":known,"recording_method":method,"scope":q.scope,"interpretation":"curated_declared_validity"}))
}

pub async fn record(State(app):State<Arc<AppState>>,tenant:Tenant,Extension(user):Extension<UserContext>,library_read::LibraryJson(mut q):library_read::LibraryJson<RecordRequest>)->Response {
    let account=match library_read::authorize_scope(&tenant,&user,&mut q.scope).await {Ok(a)=>a,Err(r)=>return r};
    if let Err(e)=validate_record(&q) {return failure(StatusCode::BAD_REQUEST,e)}
    let result=async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        crate::tenancy::scope::set_tenant_user(&mut tx,user.user_id).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        // Authoritative writable ownership, not graph-read admission alone.
        let writable:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM nodes n JOIN graphs g ON g.id=n.graph_id WHERE n.node_id=$1 AND g.owner_user_id=$2 AND g.kind IN('personal','member') AND g.archived_at IS NULL)")
            .bind(q.scope.node_id).bind(user.user_id).fetch_one(&mut *tx).await?;
        if !writable {return Ok::<_,crate::store::StoreError>(None)}
        let result=record_tx(&mut tx,user.user_id,&q,"human_curated_v1").await;
        if result.is_err() {return Ok(Some(result))}
        // Fresh admission before commit: a refused response never leaves a
        // persisted record behind for a retry to duplicate.
        for g in &q.scope.graph_ids { if !crate::source_evidence::still_admitted(&app,&tenant,&user,*g,account).await {return Ok(None)} }
        tx.commit().await?; Ok(Some(result))
    }.await;
    match result {
        Ok(Some(Ok(value)))=>response(StatusCode::CREATED,value),Ok(Some(Err(e)))=>failure(StatusCode::BAD_REQUEST,&e),Ok(None)=>failure(StatusCode::NOT_FOUND,"temporal evidence unavailable"),Err(_)=>failure(StatusCode::SERVICE_UNAVAILABLE,"temporal evidence unavailable")
    }
}

fn response(status:StatusCode,body:Value)->Response { (status,[(header::CACHE_CONTROL,"no-store")],Json(body)).into_response() }
fn failure(status:StatusCode,msg:&str)->Response { response(status,json!({"error":msg})) }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateRequest { pub scope:LibraryScope, pub temporal:TemporalSelection }
pub async fn state(State(app):State<Arc<AppState>>,tenant:Tenant,Extension(user):Extension<UserContext>,library_read::LibraryJson(mut q):library_read::LibraryJson<StateRequest>)->Response {
    let account=match library_read::authorize_scope(&tenant,&user,&mut q.scope).await {Ok(a)=>a,Err(r)=>return r};
    if !q.temporal.is_historical() {return failure(StatusCode::BAD_REQUEST,"historical state selection required")}
    if let Err(e)=q.temporal.validate(&q.scope) {return failure(StatusCode::BAD_REQUEST,e)}
    for g in &q.scope.graph_ids { if !crate::source_evidence::still_admitted(&app,&tenant,&user,*g,account).await {return failure(StatusCode::NOT_FOUND,"state unavailable")} }
    let result=async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        crate::tenancy::scope::set_tenant_user(&mut tx,user.user_id).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        if !library_read::validate_objects_tx(&mut tx,&q.scope).await? {return Ok::<_,crate::store::StoreError>(None)}
        let reconstructed=read_tx(&mut tx,&q.scope,&q.temporal).await?;
        let id=Uuid::new_v4();let graph=q.scope.graph_ids[0];
        crate::scoped_ask::insert_receipt(&mut tx,id,graph,user.user_id,&reconstructed.support()).await.map_err(|_|crate::store::StoreError::Conflict("historical receipt unavailable".into()))?;
        receipt_tx(&mut tx,id,graph,user.user_id,&q.temporal,&reconstructed).await?;
        tx.commit().await?;
        let answer=crate::scoped_ask::Answer{payload:Value::Null,scope:q.scope.clone(),account,primary_graph:graph,receipt_id:id,generated:false};
        if !crate::scoped_ask::emission_allowed(&app,&tenant,&user,&answer).await {return Ok(None)}
        Ok(Some(reconstructed.state))
    }.await;
    match result { Ok(Some(state))=> {
        for g in &q.scope.graph_ids { if !crate::source_evidence::still_admitted(&app,&tenant,&user,*g,account).await {return failure(StatusCode::NOT_FOUND,"state unavailable")} }
        response(StatusCode::OK,json!({"scope":q.scope,"temporal_state":state}))
    },Ok(None)=>failure(StatusCode::NOT_FOUND,"state unavailable"),Err(_)=>failure(StatusCode::SERVICE_UNAVAILABLE,"state unavailable") }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn at(s:&str)->DateTime<Utc>{ format!("{s}T00:00:00Z").parse().unwrap() }
    fn rec(id:u128,op:&str,value:Option<&str>,from:Option<&str>,until:Option<&str>,pred:Option<u128>,item:u128)->Record {
        Record{event_id:Uuid::from_u128(id),graph_id:Uuid::nil(),subject_node_id:Uuid::nil(),state_key:"status".into(),
            source_item_id:Uuid::from_u128(1000+item),source_version_id:Uuid::nil(),semantic_atom_id:Uuid::nil(),passage_id:Uuid::from_u128(id),
            known_at:at("2026-06-01"),observed_at:at("2026-06-01"),operation:op.into(),value:value.map(str::to_string),
            valid_from:from.map(at),valid_until:until.map(at),predecessor_event_id:pred.map(Uuid::from_u128),recording_method:"human_curated_v1".into(),
            quote:"q".into(),start_char:0,end_char:1,statement:"s".into(),kind:"decision".into(),representation:"text".into(),
            captured_at:at("2026-06-01"),occurred_at:None,source_version_is_current:true,synthetic_demo:false,confidence:None}
    }
    fn state(records:&[Record],effective:&str)->Value {
        reconstruct(records,&TemporalSelection::Historical{revision:1,state_key:"status".into(),effective_at:at(effective),as_known_at:at("2026-07-01")},false)
    }
    fn values(v:&Value)->Vec<String>{ v["values"].as_array().unwrap().iter().map(|x|x["value"].as_str().unwrap().to_string()).collect() }

    #[test]
    fn a_to_b_to_a_keeps_every_decision_and_effective_boundaries() {
        let r=[rec(1,"assert",Some("green"),Some("2026-01-01"),None,None,1),
            rec(2,"transition",Some("red"),Some("2026-03-01"),None,Some(1),2),
            rec(3,"transition",Some("green"),Some("2026-05-01"),None,Some(2),3)];
        assert_eq!(state(&r,"2025-12-01")["status"],"unknown");
        assert_eq!(values(&state(&r,"2026-02-01")),["green"]);
        assert_eq!(values(&state(&r,"2026-04-01")),["red"]);
        let s=state(&r,"2026-05-02");
        assert_eq!((s["status"].as_str(),values(&s)),(Some("supported"),vec!["green".to_string()]));
        assert_eq!(s["records"].as_array().unwrap().len(),3);
        assert_eq!(s["contradictions_resolved"],false);
    }
    #[test]
    fn independent_conflicts_and_parallel_corrections_stay_disputed() {
        let r=[rec(1,"assert",Some("green"),Some("2026-01-01"),None,None,1),rec(2,"assert",Some("red"),Some("2026-01-01"),None,None,2)];
        let s=state(&r,"2026-02-01");
        assert_eq!(s["status"],"disputed"); assert_eq!(values(&s),["green","red"]);
        let r=[rec(1,"assert",Some("green"),Some("2026-01-01"),None,None,1),
            rec(2,"correct",Some("amber"),Some("2026-01-01"),None,Some(1),2),rec(3,"correct",Some("amber"),Some("2026-01-01"),None,Some(1),3)];
        let s=state(&r,"2026-02-01");
        assert_eq!(s["status"],"disputed"); assert_eq!(s["parallel_corrections"],true);
        // Same value from distinct sources aggregates without picking either record.
        assert_eq!(s["values"][0]["source_count"],2);
    }
    #[test]
    fn corrections_replace_bounds_and_transitions_from_a_correction_end_it() {
        let r=[rec(1,"assert",Some("green"),Some("2026-01-01"),None,None,1),
            rec(2,"correct",Some("green"),Some("2026-02-01"),None,Some(1),2),
            rec(3,"transition",Some("red"),Some("2026-04-01"),None,Some(2),3)];
        assert_eq!(state(&r,"2026-01-15")["status"],"unknown");
        assert_eq!(values(&state(&r,"2026-03-01")),["green"]);
        let s=state(&r,"2026-05-01");
        assert_eq!((s["status"].as_str(),values(&s)),(Some("supported"),vec!["red".to_string()]));
    }
    #[test]
    fn withdrawal_leaves_a_gap_and_missing_validity_or_predecessor_is_incomplete() {
        let r=[rec(1,"assert",Some("green"),Some("2026-01-01"),None,None,1),rec(2,"withdraw",None,Some("2026-03-01"),None,Some(1),2)];
        assert_eq!(state(&r,"2026-04-01")["status"],"unknown");
        // Correcting the withdrawal moves its boundary.
        let mut c=r.to_vec(); c.push(rec(3,"correct",None,Some("2026-05-01"),None,Some(2),3));
        assert_eq!(values(&state(&c,"2026-04-01")),["green"]);
        assert_eq!(state(&c,"2026-06-01")["status"],"unknown");
        let r=[rec(1,"assert",Some("green"),None,None,None,1)];
        assert_eq!(state(&r,"2026-04-01")["status"],"incomplete");
        let r=[rec(2,"transition",Some("red"),Some("2026-03-01"),None,Some(99),1)];
        let s=state(&r,"2026-04-01");
        assert_eq!(s["status"],"incomplete"); assert_eq!(s["records"][0]["relation_status"],"unavailable");
        assert!(s["records"][0]["predecessor_event_id"].is_null());
        assert_eq!(state(&[],"2026-04-01")["status"],"unsupported");
    }
}
