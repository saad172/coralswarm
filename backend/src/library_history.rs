//! Retained capture history using the common Library admission/selection seam.
use crate::{
    library_read::{self, LibraryJson, LibraryScope, OutcomeVersions},
    middleware::auth::UserContext,
    source_origin::ProviderNamespace,
    tenancy::extractor::Tenant,
    AppState,
};
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRequest {
    pub scope: LibraryScope,
    pub temporal: Option<crate::temporal::TemporalSelection>,
    pub session_id: Option<String>,
    /// Qualifies `session_id`; refused without it.
    pub provider_namespace: Option<ProviderNamespace>,
    #[serde(default)]
    pub include_subagents: bool,
    pub page_size: Option<i64>,
    pub after: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCursor {
    revision: u8,
    digest: String,
    captured_at: DateTime<Utc>,
    version_id: Uuid,
}
/// Provider-local session id as captured.
struct SessionId(String);
impl SessionId {
    fn parse(raw: &str) -> Option<Self> {
        (!raw.is_empty() && raw.chars().count() <= 512 && !raw.contains('\0'))
            .then(|| Self(raw.to_string()))
    }
}
struct SessionQuery {
    id: SessionId,
    namespace: Option<ProviderNamespace>,
    include_subagents: bool,
}
fn session_query(q: &HistoryRequest) -> Result<Option<SessionQuery>, ()> {
    let Some(raw) = q.session_id.as_deref() else {
        return if q.provider_namespace.is_some() || q.include_subagents { Err(()) } else { Ok(None) };
    };
    if q.include_subagents && q.provider_namespace.is_none() {
        return Err(());
    }
    Ok(Some(SessionQuery {
        id: SessionId::parse(raw).ok_or(())?,
        namespace: q.provider_namespace.clone(),
        include_subagents: q.include_subagents,
    }))
}

/// Exactly one canonical captured session.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SessionKey {
    pub graph_id: Uuid,
    pub provider_namespace: String,
    pub session_id: String,
}
/// One (graph, namespace) group of permitted versions carrying the requested id.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SessionCandidate {
    #[serde(flatten)]
    pub key: SessionKey,
    pub version_count: u64,
    pub first_captured_at: DateTime<Utc>,
    pub last_captured_at: DateTime<Utc>,
    pub title: Option<String>,
}
#[derive(Debug, PartialEq)]
enum SessionResolution {
    Resolved(SessionKey),
    Ambiguous(Vec<SessionCandidate>),
    NotFound,
}
fn resolve(mut groups: Vec<SessionCandidate>) -> SessionResolution {
    match groups.len() {
        0 => SessionResolution::NotFound,
        1 => SessionResolution::Resolved(groups.remove(0).key),
        _ => SessionResolution::Ambiguous(groups),
    }
}
/// Source-proved subagent sessions under the requested one (same graph and
/// namespace), plus per-namespace counts of versions that claim a session in the
/// tree as parent or root but cannot prove it from another namespace.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SubagentTree {
    pub sessions: Vec<SubagentSession>,
    pub sessions_truncated: bool,
    pub excluded: Vec<NamespaceExclusion>,
    pub excluded_truncated: bool,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SubagentSession {
    pub session_id: String,
    /// The tree member this session's own capture names as parent; `None` when
    /// only its root pointer to the requested session admitted it.
    pub parent_session_id: Option<String>,
    pub version_count: u64,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NamespaceExclusion {
    pub provider_namespace: String,
    pub version_count: u64,
}
const SUBAGENT_SESSION_LIMIT: usize = 100;
const EXCLUDED_NAMESPACE_LIMIT: usize = 16;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubagentRows {
    sessions: Vec<SubagentSession>,
    excluded: Vec<NamespaceExclusion>,
}
impl From<SubagentRows> for SubagentTree {
    fn from(mut rows: SubagentRows) -> Self {
        let sessions_truncated = rows.sessions.len() > SUBAGENT_SESSION_LIMIT;
        rows.sessions.truncate(SUBAGENT_SESSION_LIMIT);
        let excluded_truncated = rows.excluded.len() > EXCLUDED_NAMESPACE_LIMIT;
        rows.excluded.truncate(EXCLUDED_NAMESPACE_LIMIT);
        Self { sessions: rows.sessions, sessions_truncated, excluded: rows.excluded, excluded_truncated }
    }
}
enum HistoryOutcome {
    Page(Value, Option<SessionKey>),
    Ambiguous(Vec<SessionCandidate>),
    NotFound,
}

fn reply(status: StatusCode, value: Value) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
}
fn fail(status: StatusCode, message: &str) -> Response {
    reply(status, json!({"error":message}))
}

pub async fn history(
    State(app): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(mut q): LibraryJson<HistoryRequest>,
) -> Response {
    let limit = q.page_size.unwrap_or(20);
    let session = match session_query(&q) {
        Ok(session) if (1..=50).contains(&limit) => session,
        _ => return fail(StatusCode::BAD_REQUEST, "invalid history selection"),
    };
    let account = match library_read::authorize_scope(&tenant, &user, &mut q.scope).await {
        Ok(a) => a,
        Err(r) => return r,
    };
    if q.scope.outcome_versions == Some(OutcomeVersions::Current) {
        return fail(
            StatusCode::BAD_REQUEST,
            "capture history requires all_retained mode",
        );
    }
    let temporal=q.temporal.clone().unwrap_or_default();
    if let Err(e)=temporal.validate(&q.scope) {return fail(StatusCode::BAD_REQUEST,e)}
    let mut cursor_selection=json!({"scope":q.scope,"session_id":q.session_id,"provider_namespace":q.provider_namespace,"include_subagents":q.include_subagents});
    if temporal.is_historical() {cursor_selection["temporal"]=json!(temporal);}
    let signature=crate::services::source_reingest::content_hash_hex(&cursor_selection.to_string());
    let after = match q.after.as_deref() {
        None => None,
        Some(raw) => {
            let parsed: Option<HistoryCursor> = if raw.len() <= 2048 {
                URL_SAFE_NO_PAD
                    .decode(raw)
                    .ok()
                    .and_then(|b| serde_json::from_slice(&b).ok())
            } else {
                None
            };
            match parsed {
                Some(c) if c.revision == 1 && c.digest == signature => Some(c),
                _ => {
                    return fail(
                        StatusCode::BAD_REQUEST,
                        "history cursor does not match request",
                    )
                }
            }
        }
    };
    let result=async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        crate::tenancy::scope::set_tenant_user(&mut tx,user.user_id).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        if !library_read::validate_objects_tx(&mut tx,&q.scope).await? {return Ok::<_,crate::store::StoreError>(HistoryOutcome::NotFound)}
        let cte=library_read::scope_cte(&q.scope);
        let projection=library_read::OUTCOME_PROJECTION;
        let sql=format!(r#"{cte}, attributed AS MATERIALIZED (
         SELECT v.*,COALESCE(v.version_id=s.current_version_id,false) source_version_is_current,sv.session_id attributed_session_id
         FROM scope_sources s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id
         LEFT JOIN LATERAL (SELECT sv.session_id FROM library_session_versions(v.graph_id) sv
          WHERE $7::text IS NOT NULL AND ($12::text IS NULL OR v.provider_namespace=$12) AND sv.version_id=v.version_id) sv ON true
         -- The reader's team-seat history floor per listed version (#1545).
         WHERE ($4::uuid IS NULL OR v.version_id=$4) AND {version_floor}
        ), members AS MATERIALIZED (
         WITH RECURSIVE walk(graph_id,session_id,parent_session_id,depth) AS (
          SELECT g.graph_id,$7::text,NULL::text,0 FROM (SELECT DISTINCT graph_id FROM attributed) g
          WHERE $8 AND library_session_supported(g.graph_id,$12,$7)
          UNION
          SELECT a.graph_id,a.attributed_session_id,
           CASE WHEN a.capture_context->>'parent_session_id'=w.session_id THEN w.session_id END,w.depth+1
          FROM walk w JOIN attributed a ON a.graph_id=w.graph_id AND a.provider_namespace=$12
           AND (a.capture_context->>'parent_session_id'=w.session_id OR (w.depth=0 AND a.capture_context->>'root_session_id'=$7))
          WHERE w.depth<32 AND a.attributed_session_id IS NOT NULL AND a.attributed_session_id<>w.session_id AND a.attributed_session_id<>$7
         )
         SELECT DISTINCT ON (graph_id,session_id) graph_id,session_id,parent_session_id FROM walk
         ORDER BY graph_id,session_id,parent_session_id IS NULL,depth,parent_session_id
        ), history_versions AS MATERIALIZED (
         SELECT a.* FROM attributed a
         WHERE $7::text IS NULL OR (($12::text IS NULL OR a.provider_namespace=$12) AND (a.attributed_session_id=$7
          OR ($8 AND EXISTS(SELECT 1 FROM members m WHERE m.graph_id=a.graph_id AND m.session_id=a.attributed_session_id))))
        ), excluded AS (
         SELECT a.provider_namespace,count(*) version_count FROM attributed a
         WHERE $8 AND a.provider_namespace<>$12 AND EXISTS(SELECT 1 FROM members m WHERE m.graph_id=a.graph_id AND (
          (m.session_id=a.capture_context->>'parent_session_id' AND NOT library_session_supported(a.graph_id,a.provider_namespace,m.session_id))
          OR (m.session_id=a.capture_context->>'root_session_id' AND NOT library_session_supported(a.graph_id,a.provider_namespace,m.session_id))))
         GROUP BY a.provider_namespace ORDER BY a.provider_namespace LIMIT {excluded_probe}
        ), subagent_sessions AS (
         SELECT m.session_id,m.parent_session_id,
          (SELECT count(*) FROM history_versions h WHERE h.graph_id=m.graph_id AND h.attributed_session_id=m.session_id) version_count
         FROM members m WHERE m.session_id<>$7 ORDER BY m.session_id,m.graph_id LIMIT {session_probe}
        ), session_groups AS (
         SELECT graph_id,provider_namespace,count(*) FILTER (WHERE attributed_session_id=$7) version_count,
          COALESCE(min(captured_at) FILTER (WHERE attributed_session_id=$7),min(captured_at)) first_captured_at,
          COALESCE(max(captured_at) FILTER (WHERE attributed_session_id=$7),max(captured_at)) last_captured_at,
          (array_agg(substring(COALESCE(capture_context->>'session_title',source_metadata->>'session_title') FOR 512) ORDER BY captured_at DESC,version_id DESC) FILTER (WHERE attributed_session_id=$7))[1] title
         FROM history_versions WHERE $7::text IS NOT NULL GROUP BY graph_id,provider_namespace
        ), version_page AS (
         SELECT * FROM history_versions WHERE $9::timestamptz IS NULL OR (captured_at,version_id)<($9,$10)
         ORDER BY captured_at DESC,version_id DESC LIMIT $11
        ), masked_page AS MATERIALIZED (SELECT v.*,library_capture_context(v.graph_id,v.version_id) ctx FROM version_page v)
        SELECT jsonb_build_object('total',(SELECT count(*) FROM history_versions),
         'subagents',CASE WHEN $8 THEN jsonb_build_object(
          'sessions',COALESCE((SELECT jsonb_agg(jsonb_build_object('session_id',m.session_id,'parent_session_id',m.parent_session_id,'version_count',m.version_count) ORDER BY m.session_id) FROM subagent_sessions m),'[]'::jsonb),
          'excluded',COALESCE((SELECT jsonb_agg(jsonb_build_object('provider_namespace',x.provider_namespace,'version_count',x.version_count) ORDER BY x.provider_namespace) FROM excluded x),'[]'::jsonb)) END,
         'session_groups',COALESCE((SELECT jsonb_agg(jsonb_build_object('graph_id',g.graph_id,'provider_namespace',g.provider_namespace,'session_id',$7::text,
          'version_count',g.version_count,'first_captured_at',g.first_captured_at,'last_captured_at',g.last_captured_at,'title',g.title)
          ORDER BY g.provider_namespace,g.graph_id) FROM session_groups g),'[]'::jsonb),
         'versions',COALESCE((SELECT jsonb_agg(jsonb_build_object(
          'graph_id',v.graph_id,'source_item_id',v.source_item_id,'source_version_id',v.version_id,
          'source_type',v.source_type,'provider_namespace',v.provider_namespace,'source_id',v.source_id,
          'source_version_is_current',v.source_version_is_current,'representation',v.representation,'origin',v.origin,
          'title',substring(COALESCE(v.capture_context->>'session_title',v.source_metadata->>'session_title',v.source_metadata->>'title') FOR 512),'captured_at',v.captured_at,'source_event_at',v.occurred_at,'synthetic_demo',COALESCE(v.origin->>'transport'='synthetic',false) OR COALESCE(v.source_metadata->'synthetic_demo'='true'::jsonb,false),'source_independence',CASE WHEN v.is_legacy_baseline OR v.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,'extraction_status',v.extraction_status,'extraction_reason',CASE WHEN v.extraction_status='blocked' AND v.extraction_error IN ('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost') THEN v.extraction_error END,'participants',v.participants[1:100],'participants_truncated',cardinality(v.participants)>100,
          'context',v.ctx,'context_status',CASE WHEN v.capture_context IS NOT NULL THEN 'captured_reported'
           WHEN v.ctx->>'session_id' IS NOT NULL OR v.ctx->>'project' IS NOT NULL OR v.ctx->>'branch' IS NOT NULL THEN 'historical_partial' ELSE 'unavailable' END,
          'outcomes',jsonb_build_object('total',(SELECT count(*) FROM scope_atoms a WHERE a.graph_id=v.graph_id AND a.source_version_id=v.version_id),
           'items',COALESCE((SELECT jsonb_agg({projection} ORDER BY a.ordinal,a.semantic_atom_id) FROM (SELECT a.* FROM scope_atoms a WHERE a.graph_id=v.graph_id AND a.source_version_id=v.version_id ORDER BY a.ordinal,a.semantic_atom_id LIMIT 8) a),'[]'::jsonb),
           'truncated',(SELECT count(*)>8 FROM scope_atoms a WHERE a.graph_id=v.graph_id AND a.source_version_id=v.version_id)),
          'passages',jsonb_build_object('total',(SELECT count(*) FROM semantic_atom_passages p JOIN scope_atoms a ON a.graph_id=p.graph_id AND a.semantic_atom_id=p.semantic_atom_id AND a.source_version_id=p.source_version_id WHERE p.graph_id=v.graph_id AND p.source_version_id=v.version_id),
           'items',COALESCE((SELECT jsonb_agg(jsonb_build_object('graph_id',v.graph_id,'source_item_id',v.source_item_id,'source_version_id',v.version_id,
            'passage_id',p.passage_id,'ordinal',p.ordinal,'start_char',p.start_char,'end_char',LEAST(p.end_char,p.start_char+4000),'text',substring(p.quote FOR 4000),
            'text_truncated',char_length(p.quote)>4000,'citation_label','Captured passage') ORDER BY p.start_char,p.passage_id)
            FROM (SELECT p.*,a.ordinal FROM semantic_atom_passages p JOIN scope_atoms a ON a.graph_id=p.graph_id AND a.semantic_atom_id=p.semantic_atom_id AND a.source_version_id=p.source_version_id WHERE p.graph_id=v.graph_id AND p.source_version_id=v.version_id ORDER BY p.start_char,p.passage_id LIMIT 8) p),'[]'::jsonb),
           'truncated',(SELECT count(*)>8 FROM semantic_atom_passages p JOIN scope_atoms a ON a.graph_id=p.graph_id AND a.semantic_atom_id=p.semantic_atom_id AND a.source_version_id=p.source_version_id WHERE p.graph_id=v.graph_id AND p.source_version_id=v.version_id)))
          ORDER BY v.captured_at DESC,v.version_id DESC) FROM masked_page v),'[]'::jsonb))"#,session_probe=SUBAGENT_SESSION_LIMIT+1,excluded_probe=EXCLUDED_NAMESPACE_LIMIT+1,version_floor=crate::history_floor::admits_version("v"));
        let mut value:Value=sqlx::query_scalar(&sql).bind(&q.scope.graph_ids).bind(q.scope.origin_filter()).bind(q.scope.source_item_id)
            .bind(q.scope.source_version_id).bind(q.scope.node_id).bind(q.scope.collection_key()).bind(session.as_ref().map(|s|s.id.0.as_str())).bind(session.as_ref().is_some_and(|s|s.include_subagents))
            .bind(after.as_ref().map(|c|c.captured_at)).bind(after.as_ref().map(|c|c.version_id)).bind(limit+1)
            .bind(session.as_ref().and_then(|s|s.namespace.as_ref()).map(ProviderNamespace::as_str)).fetch_one(&mut *tx).await?;
        let groups:Vec<SessionCandidate>=serde_json::from_value(value["session_groups"].take()).map_err(|e|crate::store::StoreError::Db(Box::new(e)))?;
        let subagents:Option<SubagentTree>=serde_json::from_value::<Option<SubagentRows>>(value["subagents"].take()).map_err(|e|crate::store::StoreError::Db(Box::new(e)))?.map(SubagentTree::from);
        value["subagents"]=json!(subagents);
        // Refused selections leave before any temporal receipt is written.
        let resolved=match session.as_ref().map(|_|resolve(groups)) {
            None=>None,
            Some(SessionResolution::Resolved(key))=>Some(key),
            Some(SessionResolution::Ambiguous(candidates))=>return Ok(HistoryOutcome::Ambiguous(candidates)),
            Some(SessionResolution::NotFound)=>return Ok(HistoryOutcome::NotFound),
        };
        let historical=if temporal.is_historical() {Some(crate::temporal::read_tx(&mut tx,&q.scope,&temporal).await?)} else {None};
        let fence=if let Some(reconstructed)=historical {
            let id=Uuid::new_v4(); let graph=q.scope.graph_ids[0];
            let support=reconstructed.support();
            crate::scoped_ask::insert_receipt(&mut tx,id,graph,user.user_id,&support).await.map_err(|_|crate::store::StoreError::Conflict("historical receipt unavailable".into()))?;
            crate::temporal::receipt_tx(&mut tx,id,graph,user.user_id,&temporal,&reconstructed).await?;
            value["temporal_state"]=reconstructed.state;
            Some(crate::scoped_ask::Answer{payload:Value::Null,scope:q.scope.clone(),account,primary_graph:graph,receipt_id:id,generated:false})
        } else {None};
        tx.commit().await?;
        if let Some(answer)=fence {if !crate::scoped_ask::emission_allowed(&app,&tenant,&user,&answer).await {return Ok(HistoryOutcome::NotFound)}}
        Ok(HistoryOutcome::Page(value,resolved))
    }.await;
    match result {
        Ok(HistoryOutcome::Page(mut value, resolved)) => {
            value.as_object_mut().expect("history object").remove("session_groups");
            let versions = value["versions"]
                .as_array_mut()
                .expect("history version array");
            let more = versions.len() > limit as usize;
            versions.truncate(limit as usize);
            for version in versions.iter_mut() {
                if let Some(items) = version["outcomes"]["items"].as_array_mut() {
                    for item in items {
                        if let Some(obj) = item.as_object_mut() {
                            obj.remove("_id");
                            obj.remove("_at");
                        }
                    }
                }
            }
            let next = if more {
                versions
                    .last()
                    .and_then(|v| {
                        Some(HistoryCursor {
                            revision: 1,
                            digest: signature,
                            captured_at: serde_json::from_value(v["captured_at"].clone()).ok()?,
                            version_id: serde_json::from_value(v["source_version_id"].clone())
                                .ok()?,
                        })
                    })
                    .map(|c| URL_SAFE_NO_PAD.encode(serde_json::to_vec(&c).expect("cursor")))
            } else {
                None
            };
            value["scope"] = json!(q.scope);
            value["selection"] = json!("retained_capture_history");
            value["session"] = json!(resolved);
            value["next_cursor"] = json!(next);
            value["truncated"] = json!(more);
            reply(StatusCode::OK, value)
        }
        Ok(HistoryOutcome::Ambiguous(candidates)) => reply(
            StatusCode::CONFLICT,
            json!({"selection":"ambiguous_session","scope":q.scope,"session_id":q.session_id,"candidates":candidates}),
        ),
        Ok(HistoryOutcome::NotFound) => fail(
            StatusCode::NOT_FOUND,
            "history selection not found or access denied",
        ),
        Err(error) => {
            tracing::warn!(error=%error,"authorized capture history unavailable");
            fail(
                StatusCode::SERVICE_UNAVAILABLE,
                "capture history unavailable",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn candidate(namespace: &str) -> SessionCandidate {
        SessionCandidate {
            key: SessionKey { graph_id: Uuid::nil(), provider_namespace: namespace.into(), session_id: "s".into() },
            version_count: 1,
            first_captured_at: DateTime::<Utc>::MIN_UTC,
            last_captured_at: DateTime::<Utc>::MIN_UTC,
            title: None,
        }
    }
    #[test]
    fn resolve_refuses_to_merge_namespaces() {
        assert_eq!(resolve(vec![]), SessionResolution::NotFound);
        assert_eq!(resolve(vec![candidate("a")]), SessionResolution::Resolved(candidate("a").key));
        assert_eq!(
            resolve(vec![candidate("a"), candidate("b")]),
            SessionResolution::Ambiguous(vec![candidate("a"), candidate("b")])
        );
    }
    fn member(id: usize) -> SubagentSession {
        SubagentSession { session_id: format!("c{id:03}"), parent_session_id: Some("s".into()), version_count: 1 }
    }
    fn exclusion(namespace: usize) -> NamespaceExclusion {
        NamespaceExclusion { provider_namespace: format!("ns{namespace:02}"), version_count: 2 }
    }
    #[test]
    fn subagent_tree_reports_truncation_only_past_the_limit() {
        let rows = |n: usize| SubagentRows { sessions: (0..n).map(member).collect(), excluded: vec![] };
        let exact = SubagentTree::from(rows(SUBAGENT_SESSION_LIMIT));
        assert_eq!((exact.sessions.len(), exact.sessions_truncated), (SUBAGENT_SESSION_LIMIT, false));
        let over = SubagentTree::from(rows(SUBAGENT_SESSION_LIMIT + 1));
        assert_eq!((over.sessions.len(), over.sessions_truncated), (SUBAGENT_SESSION_LIMIT, true));
        assert_eq!(over.sessions.last(), Some(&member(SUBAGENT_SESSION_LIMIT - 1)));
        let wide = SubagentTree::from(SubagentRows {
            sessions: vec![],
            excluded: (0..EXCLUDED_NAMESPACE_LIMIT + 3).map(exclusion).collect(),
        });
        assert_eq!((wide.excluded.len(), wide.excluded_truncated), (EXCLUDED_NAMESPACE_LIMIT, true));
        let full = SubagentTree::from(SubagentRows { sessions: vec![], excluded: (0..EXCLUDED_NAMESPACE_LIMIT).map(exclusion).collect() });
        assert!(!full.excluded_truncated);
    }
    #[test]
    fn subagent_wire_shape_names_no_excluded_session() {
        let rows: SubagentRows = serde_json::from_value(json!({
            "sessions":[{"session_id":"g","parent_session_id":null,"version_count":1}],
            "excluded":[{"provider_namespace":"codex","version_count":2}]
        }))
        .unwrap();
        let wire = serde_json::to_value(SubagentTree::from(rows)).unwrap();
        assert_eq!(
            wire,
            json!({"sessions":[{"session_id":"g","parent_session_id":null,"version_count":1}],
                "sessions_truncated":false,"excluded":[{"provider_namespace":"codex","version_count":2}],"excluded_truncated":false})
        );
        let leaked = json!({"sessions":[],"excluded":[{"provider_namespace":"codex","version_count":2,"session_id":"x"}]});
        assert!(serde_json::from_value::<SubagentRows>(leaked).is_err());
    }
    #[test]
    fn candidate_wire_shape_is_flat() {
        let wire = serde_json::to_value(candidate("a")).unwrap();
        assert_eq!(wire["provider_namespace"], "a");
        assert_eq!(wire["session_id"], "s");
        assert!(wire.get("key").is_none());
    }
}
