//! Authorized Library discovery and reusable retrieval scope. This module
//! exposes retained evidence, never unattributable node prose or summaries.
pub mod evidence;
pub(crate) mod listing_facts;
pub mod read_model;
mod candidate_program;
pub use candidate_program::CandidateProgramError;
mod live_fallback;
use live_fallback::{log_live_fallback, LiveFallbackEvent, PathDecision};

use crate::{
    middleware::auth::UserContext,
    source_origin::{
        CaptureConnectionRef, ContainerId, ContainerKind, ContentKind, ProviderNamespace, ServiceId,
    },
    tenancy::extractor::Tenant,
};
use axum::{
    extract::{FromRequest, Request},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// Keep malformed JSON on the same no-store, 400 boundary as invalid scopes.
pub struct LibraryJson<T>(pub T);
impl<S, T> FromRequest<S> for LibraryJson<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned + Send,
{
    type Rejection = Response;
    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(|_| failure(StatusCode::BAD_REQUEST, "malformed library request"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerScope {
    pub kind: ContainerKind,
    pub id: ContainerId,
    pub provider_namespace: ProviderNamespace,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LibraryCollection {
    Meetings,
    AgentSessions,
    WorkSessions,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeVersions {
    Current,
    AllRetained,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LibraryScope {
    pub graph_ids: Vec<Uuid>,
    pub outcome_versions: Option<OutcomeVersions>,
    pub service_id: Option<ServiceId>,
    pub connection: Option<CaptureConnectionRef>,
    pub container: Option<ContainerScope>,
    pub content_kind: Option<ContentKind>,
    pub collection: Option<LibraryCollection>,
    pub source_item_id: Option<Uuid>,
    pub source_version_id: Option<Uuid>,
    pub node_id: Option<Uuid>,
}
impl LibraryScope {
    /// One graph and one node with no source filters. Disagreement sides are
    /// node-level facts; a source filter would make "filtered out"
    /// indistinguishable from "not readable".
    pub fn exact_node(graph_id: Uuid, node_id: Uuid) -> Self {
        Self {
            graph_ids: vec![graph_id],
            outcome_versions: None,
            service_id: None,
            connection: None,
            container: None,
            content_kind: None,
            collection: None,
            source_item_id: None,
            source_version_id: None,
            node_id: Some(node_id),
        }
    }
    pub fn normalize(&mut self) -> Result<(), &'static str> {
        if self.graph_ids.is_empty() || self.graph_ids.len() > 32 {
            return Err("invalid graph scope");
        }
        self.graph_ids.sort();
        self.graph_ids.dedup();
        // An exact immutable snapshot is stronger than an aggregate mode.
        if self.source_version_id.is_some() || self.outcome_versions.is_none() {
            self.outcome_versions = Some(OutcomeVersions::AllRetained);
        }
        if (self.connection.is_some() || self.container.is_some()) && self.service_id.is_none() {
            return Err("service required for connection or container");
        }
        Ok(())
    }
    /// Ask reads each item's served version only, whatever `outcome_versions`
    /// the caller sent. Earlier retained versions repeat the served facts or
    /// predate them, and ranking them once per re-capture quadrupled the
    /// candidate set. History needs an exact version scope or a temporal read.
    /// Receipt checks keep the caller's scope, a superset, so citations from
    /// earlier turns stay valid after a re-capture.
    pub fn serve_current(&mut self) {
        if self.source_version_id.is_none() {
            self.outcome_versions = Some(OutcomeVersions::Current);
        }
    }
    pub(crate) fn origin_filter(&self) -> Value {
        let mut v = json!({});
        if let Some(s) = &self.service_id {
            v["service_id"] = json!(s);
        }
        if let Some(c) = &self.connection {
            v["connection"] = json!(c);
        }
        if let Some(c) = &self.container {
            v["container"] = json!(c);
        }
        if let Some(c) = &self.content_kind {
            v["content_kind"] = json!(c);
        }
        v
    }
    pub(crate) fn collection_key(&self) -> Option<&'static str> {
        self.collection.as_ref().map(|c| match c {
            LibraryCollection::Meetings => "meetings",
            LibraryCollection::AgentSessions => "agent_sessions",
            LibraryCollection::WorkSessions => "work_sessions",
        })
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub scope: LibraryScope,
    #[serde(default)]
    pub query: String,
    pub page_size: Option<i64>,
    pub sources_after: Option<String>,
    pub outcomes_after: Option<String>,
    pub nodes_after: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    revision: u8,
    group: String,
    digest: String,
    id: Uuid,
    at: Option<DateTime<Utc>>,
    /// Outcome pages order by source event time first; a null event time sorts last.
    occurred: Option<DateTime<Utc>>,
    sources: Option<i64>,
    outcomes: Option<i64>,
}
fn digest(scope: &LibraryScope, query: &str) -> String {
    crate::services::source_reingest::content_hash_hex(
        &json!({"scope":scope,"query":query}).to_string(),
    )
}
/// Revision 2 changed outcome order to source event time first.
const CURSOR_REVISION: u8 = 2;
fn parse_cursor(
    value: Option<&str>,
    group: &str,
    digest: &str,
) -> Result<Option<Cursor>, &'static str> {
    let Some(value) = value else { return Ok(None) };
    if value.len() > 2048 {
        return Err("malformed cursor");
    }
    let c: Cursor = URL_SAFE_NO_PAD
        .decode(value)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or("malformed cursor")?;
    if c.revision != CURSOR_REVISION
        || c.group != group
        || c.digest != digest
        || c.sources.is_some_and(|n| n < 0)
        || c.outcomes.is_some_and(|n| n < 0)
    {
        return Err("cursor does not match request");
    }
    if group == "nodes" && (c.sources.is_none() || c.outcomes.is_none()) {
        return Err("malformed node cursor");
    }
    if matches!(group, "outcomes" | "node_sessions") && c.at.is_none() {
        return Err("malformed outcome cursor");
    }
    Ok(Some(c))
}
fn page_size(value: Option<i64>) -> Result<i64, &'static str> {
    let n = value.unwrap_or(20);
    if !(1..=50).contains(&n) {
        Err("invalid page size")
    } else {
        Ok(n)
    }
}
fn pattern(query: &str) -> Option<String> {
    (!query.is_empty()).then(|| {
        format!(
            "%{}%",
            query
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
        )
    })
}
/// Apply to compatible source/version routes as well as new Library handlers.
pub async fn no_store(request: Request, next: axum::middleware::Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

fn response(status: StatusCode, value: Value) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
}
fn failure(status: StatusCode, message: &str) -> Response {
    response(status, json!({"error":message}))
}

/// Validate each graph against actual reachability and keep one ownership
/// boundary. An echoed scope is descriptive, never an authorization token.
pub async fn authorize_scope(
    tenant: &Tenant,
    user: &UserContext,
    scope: &mut LibraryScope,
) -> Result<Uuid, Response> {
    scope
        .normalize()
        .map_err(|e| failure(StatusCode::BAD_REQUEST, e))?;
    let mut owner = None;
    for g in &scope.graph_ids {
        let a = crate::source_evidence::authorize(tenant, user, *g)
            .await
            .map_err(|(s, Json(v))| response(s, v))?;
        if owner.is_some_and(|known| known != a) {
            return Err(failure(
                StatusCode::BAD_REQUEST,
                "scope spans account boundaries",
            ));
        }
        owner = Some(a);
    }
    Ok(owner.expect("normalized nonempty scope"))
}

/// Common source selector. Parameters 1–6 are graph IDs, origin filter, exact
/// item, exact version, exact node and collection. All text follows admission;
/// null origins never match a nonempty origin filter.
///
/// Every set here is built by joins and grouped aggregates, never by a
/// per-row permission call or correlated count. A whole-ocean scope holds
/// tens of thousands of nodes, and each per-node `library_node_permitted`,
/// `library_atom_permitted` or count subquery cost about a millisecond, which
/// put the 5 s request budget out of reach (154 s measured on a 55k-node
/// graph). Node membership proves permission by construction: each route
/// into `scope_nodes` passes through a permitted source item or
/// `library_permitted_atoms`. `raw_title` is the stored title before evidence
/// authorization. It serves text matching only; every emitted title comes
/// from `library_node_title`, evaluated on the page rows alone.
const SCOPE_CTE: &str = r#"
WITH scope_graph_chunks AS MATERIALIZED (
 -- The scope graphs' chunk mappings and versions, one scan each. The
 -- permitted-atom set is a function scan the planner prices at 500 rows, so
 -- every join from it became a per-row index probe: 36k chunk probes for
 -- `unmapped` and 23k more plus 41k version probes for `scoped_chunk_atoms`
 -- (0.4 s on the 2026-10-23 org tenant) against 30 ms for the two scans.
 SELECT c.graph_id,c.atom_id,c.source_version_id FROM source_version_chunks c WHERE c.graph_id=ANY($1)
), scope_graph_versions AS MATERIALIZED (
 SELECT v.graph_id,v.version_id,v.source_item_id,v.is_legacy_baseline,v.source_metadata,v.origin FROM source_versions v WHERE v.graph_id=ANY($1)
), scoped_permitted_atoms AS MATERIALIZED (
 SELECT g.graph_id,p.atom_id,NOT EXISTS(SELECT 1 FROM scope_graph_chunks c WHERE c.graph_id=g.graph_id AND c.atom_id=p.atom_id) unmapped
 FROM (SELECT DISTINCT unnest($1::uuid[]) graph_id) g CROSS JOIN LATERAL library_permitted_atoms(g.graph_id,NULL) p
), scope_node_items AS MATERIALIZED (
 SELECT DISTINCT l.graph_id,mapped.source_item_id,mapped.version_id FROM node_semantic_atoms l
  JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
  JOIN source_versions mapped ON mapped.version_id=a.source_version_id AND mapped.graph_id=a.graph_id
  WHERE l.graph_id=ANY($1) AND l.node_id=$5 AND public.history_floor_admits(mapped.graph_id,COALESCE(mapped.occurred_at,mapped.captured_at),(SELECT public.reader_history_floors()))
 UNION
 SELECT DISTINCT c.graph_id,mapped.source_item_id,mapped.version_id FROM node_atoms l
  JOIN scope_graph_chunks c ON c.atom_id=l.atom_id
  JOIN scoped_permitted_atoms p ON p.graph_id=c.graph_id AND p.atom_id=c.atom_id
  JOIN scope_graph_versions mapped ON mapped.version_id=c.source_version_id AND mapped.graph_id=c.graph_id
  WHERE l.node_id=$5
), scope_sources AS MATERIALIZED (
 SELECT s.*,CASE WHEN s.origin IS NULL THEN s.source_type ELSE s.origin->>'content_kind' END discovery_content_kind,v.version_id selected_version_id,v.origin version_origin,v.source_metadata,
 v.representation,v.occurred_at,v.captured_at,v.is_legacy_baseline,
 __SELECTED_EXTRACTION_STATUS__ selected_extraction_status,__CURRENT_EXTRACTION_STATUS__ current_extraction_status,CASE WHEN v.extraction_status='blocked' AND v.extraction_error IN ('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost') THEN v.extraction_error END extraction_error,CASE WHEN current_v.extraction_status='blocked' AND current_v.extraction_error IN ('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost') THEN current_v.extraction_error END current_extraction_error,
 current_v.captured_at current_captured_at,
 substring(NULLIF(v.source_metadata->>'title','') FOR 256) title
 FROM source_items s LEFT JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id
  AND v.version_id=COALESCE($4::uuid,s.current_version_id)
 LEFT JOIN source_versions current_v ON current_v.graph_id=s.graph_id AND current_v.source_item_id=s.source_item_id AND current_v.version_id=s.current_version_id
 WHERE s.graph_id=ANY($1) AND s.access_state='permitted'
  AND ($2::jsonb='{}'::jsonb OR s.origin @> $2 OR (s.origin IS NULL AND $2::jsonb=jsonb_build_object('content_kind',s.source_type)))
  AND ($3::uuid IS NULL OR s.source_item_id=$3)
  AND ($4::uuid IS NULL OR v.version_id=$4)
  AND ($6::text IS NULL OR CASE $6
   WHEN 'agent_sessions' THEN s.source_type IN ('coding_session','agent_session') OR s.origin->>'content_kind' IN ('coding_session','agent_session')
   WHEN 'work_sessions' THEN s.source_type='work_session' OR s.origin->>'content_kind'='work_session'
   WHEN 'meetings' THEN s.source_type IN ('transcript','meeting_session','teams_transcript','zoom_transcript','google_meet_transcript','slack_huddle_summary') OR s.origin->>'content_kind' IN ('transcript','meeting','meeting_session')
   ELSE false END)
  AND ($5::uuid IS NULL OR EXISTS(SELECT 1 FROM scope_node_items i
   WHERE i.graph_id=s.graph_id AND i.source_item_id=s.source_item_id AND ($4::uuid IS NULL OR i.version_id=$4)))
  -- The reader's team-seat history floor on the item at its shown version (#1545).
  AND __SOURCE_ITEM_FLOOR__
), scope_served_versions AS MATERIALIZED (
 __SERVED_VERSIONS__
), retained_scope_atoms AS MATERIALIZED (
 -- Outcomes are read per scoped version through its index (OFFSET 0 keeps
 -- the lateral from flattening into a join): as a join on version the
 -- planner hashed the scope's versions and read the whole semantic_atoms
 -- table for them, 349k rows and 420 ms for one ocean's 27k on the
 -- 2026-10-23 org tenant, on every Library read.
 SELECT a.*,v.source_item_id,v.origin,v.occurred_at,v.captured_at,v.representation,v.source_metadata,v.is_legacy_baseline,
 COALESCE(a.source_version_id=COALESCE(served.version_id,s.current_version_id),false) source_version_is_current,s.source_type,s.title
 FROM scope_sources s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id
 LEFT JOIN scope_served_versions served ON served.graph_id=s.graph_id AND served.source_item_id=s.source_item_id
 -- A selected node's links are one hashed set (the IN subquery has no outer
 -- reference), not a probe per outcome of the node's items.
 CROSS JOIN LATERAL (SELECT a.* FROM semantic_atoms a WHERE a.graph_id=v.graph_id AND a.source_version_id=v.version_id
  AND ($5::uuid IS NULL OR (a.graph_id,a.semantic_atom_id) IN (SELECT l.graph_id,l.semantic_atom_id FROM node_semantic_atoms l WHERE l.node_id=$5)) OFFSET 0) a
 -- The reader's team-seat history floor (crate::history_floor), per version.
 WHERE ($4::uuid IS NULL OR v.version_id=$4) AND public.history_floor_admits(v.graph_id,COALESCE(v.occurred_at,v.captured_at),(SELECT public.reader_history_floors()))
), scope_atoms AS MATERIALIZED (
__SCOPE_ATOMS__
), __EVIDENCE_CTES__, scope_graph_nodes AS MATERIALIZED (
 -- The scope graphs' node keys from one index-only scan. Joined per
 -- admitted node instead, the planner's one-row guess for the metrics set
 -- probed the index 66k times (0.9 s on the 2026-10-23 org tenant, where a
 -- whole-ocean scope admits every node of the graph).
 SELECT n.node_id,n.graph_id FROM nodes n WHERE n.graph_id=ANY($1) AND ($5::uuid IS NULL OR n.node_id=$5)
), scope_nodes AS MATERIALIZED (
 -- Keys and metrics only. The join proves the node exists in the scope's
 -- graph; readers that show a node's own columns fetch them for their page
 -- rows (`__NODE_COLUMNS__`). Carrying them here fetched every admitted
 -- node's row: 66k heap probes into a 450 MB table, 0.6 s, on the
 -- 2026-10-23 org tenant.
 SELECT support.node_id,support.graph_id,
  support.source_count,support.unknown_independence_record_count,support.atom_count,support.current_atom_count,support.unmapped_legacy_count
 FROM scope_node_metrics support JOIN scope_graph_nodes n ON n.graph_id=support.graph_id AND n.node_id=support.node_id
 WHERE support.admitted
)
"#;

// Same authoritative current-window selector as source_evidence::extraction_state:
// completed versions win; process death may leave the row stale while the exact
// next-window job is pending/running/dead. No queue error/provider text escapes.
const EXTRACTION_STATUS_SQL: &str = r#"CASE WHEN __VERSION__.extraction_status='done' THEN 'done'
 ELSE COALESCE((SELECT CASE j.status WHEN 'pending' THEN 'pending' WHEN 'running' THEN 'running'
 WHEN 'dead' THEN 'failed' ELSE __VERSION__.extraction_status END FROM jobs j
 WHERE j.kind IN('ExtractMeetingAtoms','ExtractSourceAtoms')
 AND j.payload->>'source_version_id'=__VERSION__.version_id::text
 AND j.payload->>'graph_id'=__VERSION__.graph_id::text
 AND COALESCE(j.payload->>'start_char','0')=__VERSION__.next_start_char::text
 ORDER BY j.created_at DESC,j.id DESC LIMIT 1),__VERSION__.extraction_status) END"#;

/// Per scoped item, the version whose facts its current version shows when
/// that is an earlier version: the current version has no facts of its own,
/// or its extraction has not finished, and an earlier version of the same
/// item holds byte-identical content in the same representation with
/// extracted facts. That is a metadata-only re-capture
/// (`source_evidence::carries_extraction`), or one still waiting for
/// extraction. Same item only, so access and retraction stay per item.
/// Changed content never matches; it waits for its own extraction. Set form
/// over `scope_sources` (a per-item lateral cost 4x as much on prod).
const SERVED_VERSIONS_SQL: &str = r#"SELECT DISTINCT ON (c.graph_id,c.source_item_id) c.graph_id,c.source_item_id,p.version_id
 -- history_floor: keys only, from the floored scope_sources; the served
 -- version's atoms are floored where they are read (SERVED_SCOPE_ATOMS_SQL).
 FROM scope_sources s JOIN source_versions c ON c.graph_id=s.graph_id AND c.version_id=s.current_version_id
 JOIN source_versions p ON p.graph_id=c.graph_id AND p.source_item_id=c.source_item_id AND p.content_sha256=c.content_sha256
  AND p.representation=c.representation AND p.version_id<>c.version_id
 -- OFFSET 0 keeps each existence test a per-version probe: pulled up into
 -- semi-joins, PostgreSQL 16 joined the scope's sources to the graph's
 -- outcome versions on graph_id alone first (4k by 10k rows, 40 s on the
 -- p6 facets fixture) before the keyed filters.
 WHERE (c.extraction_status<>'done' OR NOT EXISTS(SELECT 1 FROM semantic_atoms x WHERE x.graph_id=c.graph_id AND x.source_version_id=c.version_id OFFSET 0))
  AND EXISTS(SELECT 1 FROM semantic_atoms x WHERE x.graph_id=p.graph_id AND x.source_version_id=p.version_id OFFSET 0)
 ORDER BY c.graph_id,c.source_item_id,p.extraction_status='done' DESC,p.captured_at DESC,p.version_id DESC"#;

/// `scope_atoms` for a served-versions read: each item's served version joined
/// to its atoms by version id, the same columns `retained_scope_atoms` carries.
const SERVED_SCOPE_ATOMS_SQL: &str = r#" SELECT a.*,v.source_item_id,v.origin,v.occurred_at,v.captured_at,v.representation,v.source_metadata,v.is_legacy_baseline,
 true source_version_is_current,s.source_type,s.title
 FROM scope_sources s LEFT JOIN scope_served_versions served ON served.graph_id=s.graph_id AND served.source_item_id=s.source_item_id
 JOIN source_versions v ON v.graph_id=s.graph_id AND v.version_id=COALESCE(served.version_id,s.current_version_id)
 JOIN semantic_atoms a ON a.graph_id=v.graph_id AND a.source_version_id=v.version_id
 WHERE ($5::uuid IS NULL OR (a.graph_id,a.semantic_atom_id) IN (SELECT l.graph_id,l.semantic_atom_id FROM node_semantic_atoms l WHERE l.node_id=$5))
  AND public.history_floor_admits(v.graph_id,COALESCE(v.occurred_at,v.captured_at),(SELECT public.reader_history_floors()))"#;

/// Static typed selection keeps the six existing source-admission binds stable.
/// Current is captured-pointer membership; it never asserts effective validity.
fn reads_served_versions_only(scope: &LibraryScope) -> bool {
    scope.source_version_id.is_none() && scope.outcome_versions == Some(OutcomeVersions::Current)
}

pub(crate) fn scope_cte(scope: &LibraryScope) -> String {
    // Served versions are joined to their atoms by version id, so an ocean's
    // retained history is never materialised for a read that does not show it.
    let scope_atoms = if reads_served_versions_only(scope) {
        SERVED_SCOPE_ATOMS_SQL
    } else {
        " SELECT a.* FROM retained_scope_atoms a"
    };
    SCOPE_CTE
        .replace("__EVIDENCE_CTES__", &evidence::evidence_ctes(&evidence::SCOPE_BINDS))
        .replace("__SCOPE_ATOMS__", scope_atoms)
        .replace("__SOURCE_ITEM_FLOOR__", &crate::history_floor::admits_source_item_at("s", "v"))
        .replace("__SERVED_VERSIONS__", SERVED_VERSIONS_SQL)
        .replace(
            "__SELECTED_EXTRACTION_STATUS__",
            &EXTRACTION_STATUS_SQL.replace("__VERSION__", "v"),
        )
        .replace(
            "__CURRENT_EXTRACTION_STATUS__",
            &EXTRACTION_STATUS_SQL.replace("__VERSION__", "current_v"),
        )
}

pub async fn validate_objects_tx(
    tx: &mut Transaction<'_, Postgres>,
    scope: &LibraryScope,
) -> Result<bool, sqlx::Error> {
    let exact_node:bool=sqlx::query_scalar("SELECT $2::uuid IS NULL OR EXISTS(SELECT 1 FROM nodes WHERE graph_id=ANY($1) AND node_id=$2 AND library_node_permitted(graph_id,node_id))")
        .bind(&scope.graph_ids).bind(scope.node_id).fetch_one(&mut **tx).await?;
    if !exact_node {
        return Ok(false);
    }
    if scope.source_item_id.is_none() && scope.source_version_id.is_none() {
        return Ok(true);
    }
    let query = format!(
        "{} SELECT EXISTS(SELECT 1 FROM scope_sources)",
        scope_cte(scope)
    );
    sqlx::query_scalar(&query)
        .bind(&scope.graph_ids)
        .bind(scope.origin_filter())
        .bind(scope.source_item_id)
        .bind(scope.source_version_id)
        .bind(scope.node_id)
        .bind(scope.collection_key())
        .fetch_one(&mut **tx)
        .await
}

pub async fn search(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(q): LibraryJson<SearchRequest>,
) -> Response {
    match read_scoped_search(&tenant, &user, q).await {
        Ok(value) => response(StatusCode::OK, value),
        Err(response) => response,
    }
}

/// Reusable server-side P7 boundary. It returns the same bounded DTO as search,
/// including explicit continuation/truncation, with fresh graph/object admission.
/// Callers must use selected-scope outcomes/citations for evidence; wider_context
/// is descriptive. This is not an authorization token for later retrieval or
/// model persistence, which must revalidate live access before emission.
pub async fn retrieve_scoped(
    tenant: &Tenant,
    user: &UserContext,
    scope: LibraryScope,
    query: &str,
    limit: i64,
) -> Result<Value, Response> {
    read_scoped_search(
        tenant,
        user,
        SearchRequest {
            scope,
            query: query.to_string(),
            page_size: Some(limit),
            sources_after: None,
            outcomes_after: None,
            nodes_after: None,
        },
    )
    .await
}

/// The candidate transaction's settings, in the one query that already sets
/// its timeout. The candidate statement is prepared and reused per
/// connection; its generic plan loses the bind-dependent folding of the
/// vector and window legs and ran 88 s on the 2026-10-21 corpus against 2 s
/// custom, so planning stays per execution. Its three window sorts over tens
/// of thousands of matches spill at the 4 MB default `work_mem`. No plan of
/// it on prod data launched a parallel worker, and a parallel hash join
/// sized by that `work_mem` claims a shared-memory segment the test
/// containers' 64 MB `/dev/shm` cannot hold, so workers stay off.
/// JIT is off: with it on, a PostgreSQL that estimates these statements above
/// its JIT thresholds compiles hundreds of expressions per execution (1.3 s
/// per receipt fence on the pg16 test image); prod's plans never used it.
const CANDIDATE_TX_SETTINGS_SQL: &str = "SELECT set_config('statement_timeout','5s',true),\
 set_config('plan_cache_mode','force_custom_plan',true),set_config('work_mem','32MB',true),\
 set_config('max_parallel_workers_per_gather','0',true),set_config('jit','off',true)";

/// The search read's statement settings: the candidate settings' planner
/// choices, with `work_mem` at 64 MB because `retained_scope_atoms`
/// (every retained outcome of the scope with its version columns, 33 MB on
/// the 2026-10-23 org tenant) spilled to disk at 32 MB and is rescanned by
/// each reader that joins it. One statement, so the allowance is per sort or
/// hash of this read alone. The Model-first program applies the same settings
/// to its conditional canonical stage; its whole wall is bounded separately.
///
/// The budget is [`SEARCH_STATEMENT_TIMEOUT`], not the 5 s every other
/// Library read uses (and not the org plane's 10 s request default, which
/// `begin_member_scoped` sets first and this overrides, transaction-locally).
/// It is an INTERIM: on the 2026-10-24 prod org tenant a whole-ocean search
/// takes 4.8-5.3 s, right on a 5 s timeout, so Library answered 503 "library
/// read unavailable". The fix is a precomputed per-node metrics read model;
/// until it lands, this one read gets the headroom to finish.
///
/// 12 s and not more because the web client aborts each attempt at 15 s
/// (`web/lib/transport.ts` `requestTimeoutMs`, measured from slot
/// acquisition), and the request also pays auth, scope authorization, BEGIN,
/// this statement and `validate_objects_tx` before the search statement
/// starts. A timeout at or above the client's would let the server keep a
/// pooled connection busy for an answer nobody is waiting for. No server-side
/// HTTP timeout layer exists (`rg TimeoutLayer backend/src`) and Fly's
/// `[http_service]` sets none.
const SEARCH_TX_SETTINGS_SQL: &str = "SELECT set_config('statement_timeout','12s',true),\
 set_config('plan_cache_mode','force_custom_plan',true),set_config('work_mem','64MB',true),\
 set_config('max_parallel_workers_per_gather','0',true),set_config('jit','off',true)";
/// The served request's planner/memory settings, for a comparison
/// transaction a test opened itself.
pub async fn configure_read_model_proof_tx(tx:&mut Transaction<'_,Postgres>)->Result<(),sqlx::Error> {
    sqlx::query(SEARCH_TX_SETTINGS_SQL).execute(&mut **tx).await?;
    Ok(())
}
/// See [`SEARCH_TX_SETTINGS_SQL`]. The `statement_timeout` the Library search
/// transaction runs with, as `current_setting` reports it; the pg suite
/// asserts the two agree.
pub const SEARCH_STATEMENT_TIMEOUT: &str = "12s";

/// Open the Library search read's transaction: the member-scoped GUCs, then
/// [`SEARCH_TX_SETTINGS_SQL`]. The Model program selects explicit read-only
/// RR before any scoped-GUC read; the Off kill switch keeps the Live opener.
async fn begin_search_tx(
    tenant: &Tenant,
    account: Uuid,
    model_program: bool,
) -> Result<Transaction<'static, Postgres>, crate::store::StoreError> {
    let mut tx = if model_program {tenant.begin_member_repeatable_read(account).await?}else{tenant.begin_member_scoped(account).await?};
    sqlx::query(SEARCH_TX_SETTINGS_SQL).execute(&mut *tx).await?;
    Ok(tx)
}
#[cfg(feature = "test-support")]
pub async fn begin_search_tx_for_test(
    tenant: &Tenant,
    account: Uuid,
) -> Result<Transaction<'static, Postgres>, crate::store::StoreError> {
    begin_search_tx(tenant, account, false).await
}

/// The served Model opener, without changing process mode.
#[cfg(feature="test-support")]
pub async fn begin_model_search_tx_for_test(tenant:&Tenant,account:Uuid)->Result<Transaction<'static,Postgres>,crate::store::StoreError> {
    begin_search_tx(tenant,account,true).await
}

/// Interleave a committed writer only after an actual successful NeedsLive.
/// The SQL builders and executor are exactly those used by served requests.
#[cfg(feature="test-support")]
pub async fn candidate_program_interleaved_for_test<F:std::future::Future<Output=()>+Send>(
    tx:&mut Transaction<'_,Postgres>,scope:&LibraryScope,query:&str,before_live:impl FnOnce()->F+Send,
)->Result<Value,CandidateProgramError> {
    let result=candidate_program::run(tx,candidate_program::Arguments{scope,query,limit:40,sources:None,outcomes:None,nodes:None},
        std::time::Duration::from_millis(read_model::PROGRAM_WALL_MILLIS),||read_model::sql(scope,query),||search_sql_with_query(scope,query),before_live).await?;
    Ok(json!({"value":result.value,"path":result.path,"fallback_reason":result.reason,"freshness":result.freshness}))
}

/// The dirty admission's verdict and overlaid keys for one scope, in the
/// caller's read snapshot (`read_model::overlay_keys_sql`).
#[cfg(feature="test-support")]
pub async fn overlay_keys_for_test(tx:&mut Transaction<'_,Postgres>,scope:&LibraryScope)->Result<Value,sqlx::Error> {
    sqlx::query_scalar(&read_model::overlay_keys_sql())
        .bind(&scope.graph_ids).bind(scope.origin_filter()).bind(scope.source_item_id).bind(scope.source_version_id)
        .bind(scope.node_id).bind(scope.collection_key()).fetch_one(&mut **tx).await
}

/// Controlled SQL responses exercise real server errors/cancellation through
/// the actual executor. Absent from builds without test-support.
#[cfg(feature="test-support")]
pub async fn candidate_program_sql_for_test(tx:&mut Transaction<'_,Postgres>,scope:&LibraryScope,budget:std::time::Duration,model:String,live:String)->Result<Value,CandidateProgramError> {
    let result=candidate_program::run(tx,candidate_program::Arguments{scope,query:"",limit:40,sources:None,outcomes:None,nodes:None},budget,||model,||live,||async{}).await?;
    Ok(json!({"value":result.value,"path":result.path,"fallback_reason":result.reason,"freshness":result.freshness}))
}

async fn configure_selected_node_planning_tx(
    tx: &mut Transaction<'_, Postgres>,
    scope: &LibraryScope,
) -> Result<(), sqlx::Error> {
    if scope.node_id.is_some() {
        sqlx::query("SET LOCAL jit=off").execute(&mut **tx).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTimeWindow {
    pub since: chrono::DateTime<chrono::Utc>,
    pub until: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecapCursor {
    revision: u8,
    digest: String,
    window: EventTimeWindow,
    event_at: DateTime<Utc>,
    graph_id: Uuid,
    record_kind: String,
    record_id: Uuid,
}

fn recap_digest(scope:&LibraryScope,user:&UserContext,account:Uuid,query:&str,window:&EventTimeWindow,limit:i64)->String {
    crate::services::source_reingest::content_hash_hex(
        &json!({
            "revision":2,"group":"event_recap","scope":scope,"query":query,"user_id":user.user_id,
            "account_id":account,"since":window.since,"until":window.until,"limit":limit
        })
        .to_string(),
    )
}
pub(crate) fn recap_interval_is_current(bound:&EventTimeWindow,fresh:&EventTimeWindow)->bool {
    bound.since==fresh.since && bound.until>bound.since && bound.until<=fresh.until
}
fn parse_recap_after(
    after:Option<&str>,scope:&LibraryScope,user:&UserContext,account:Uuid,
    query:&str,fresh:&EventTimeWindow,limit:i64,
)->Result<Option<RecapCursor>,Response> {
    Ok(match after {
        None => None,
        Some(encoded) => {
            let cursor: Option<RecapCursor> = if encoded.len() <= 4096 {
                URL_SAFE_NO_PAD
                    .decode(encoded)
                    .ok()
                    .and_then(|raw| serde_json::from_slice(&raw).ok())
            } else {
                None
            };
            match cursor {
                Some(cursor)
                    if cursor.revision == 2
                        && recap_interval_is_current(&cursor.window,fresh)
                        && cursor.digest == recap_digest(scope,user,account,query,&cursor.window,limit)
                        && matches!(
                            cursor.record_kind.as_str(),
                            "retained" | "legacy_headline"
                        )
                        && cursor.event_at >= cursor.window.since
                        && cursor.event_at < cursor.window.until =>
                {
                    Some(cursor)
                }
                _ => {
                    return Err(failure(
                        StatusCode::BAD_REQUEST,
                        "recap cursor does not match request",
                    ))
                }
            }
        }
    })
}
pub(crate) fn validate_recap_after(scope:&LibraryScope,user:&UserContext,account:Uuid,query:&str,window:Option<&EventTimeWindow>,after:Option<&str>,limit:i64)->Result<Option<EventTimeWindow>,Response> {
    if window.is_some_and(|w|w.until<=w.since) {return Err(failure(StatusCode::BAD_REQUEST,"empty recap interval"));}
    let Some(encoded)=after else {return Ok(window.cloned());};
    let window=window.ok_or_else(||failure(StatusCode::BAD_REQUEST,"recap cursor requires an ordinary event-time recap"))?;
    parse_recap_after(Some(encoded),scope,user,account,query,window,limit)
        .map(|cursor|cursor.map(|c|c.window))
}
fn encode_recap_cursor(
    scope:&LibraryScope,user:&UserContext,account:Uuid,query:&str,window:&EventTimeWindow,limit:i64,
    event_at:DateTime<Utc>,graph_id:Uuid,record_kind:String,record_id:Uuid,
)->String {
    let cursor=RecapCursor {revision:2,digest:recap_digest(scope,user,account,query,window,limit),window:window.clone(),event_at,graph_id,record_kind,record_id};
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).expect("recap cursor"))
}
#[cfg(feature="test-support")]
pub fn test_recap_cursor(
    scope:&LibraryScope,user:&UserContext,account:Uuid,query:&str,window:&EventTimeWindow,limit:i64,
    event_at:DateTime<Utc>,graph_id:Uuid,record_id:Uuid,
)->String {
    encode_recap_cursor(scope,user,account,query,window,limit,event_at,graph_id,"legacy_headline".into(),record_id)
}
#[cfg(feature="test-support")]
pub fn test_validate_recap_cursor(
    scope:&LibraryScope,user:&UserContext,account:Uuid,query:&str,window:Option<&EventTimeWindow>,after:&str,limit:i64,
)->Result<Option<EventTimeWindow>,u16> {
    validate_recap_after(scope,user,account,query,window,Some(after),limit).map_err(|r|r.status().as_u16())
}

pub(crate) async fn retrieve_recap_candidates(
    tenant: &Tenant,
    user: &UserContext,
    mut scope: LibraryScope,
    query: &str,
    window: &EventTimeWindow,
    after: Option<&str>,
    limit: i64,
) -> Result<Value, Response> {
    if !(1..=193).contains(&limit) {
        return Err(failure(StatusCode::BAD_REQUEST, "invalid recap page size"));
    }
    let account = authorize_scope(tenant, user, &mut scope).await?;
    // Recap cursors digest the caller's scope, so it stays as sent here.
    let after=parse_recap_after(after,&scope,user,account,query,window,limit)?;
    let mut served = scope.clone();
    served.serve_current();
    let result = async {
        let mut tx = tenant.begin_member_scoped(account).await?;
        crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id).await?;
        sqlx::query(CANDIDATE_TX_SETTINGS_SQL).execute(&mut *tx).await?;
        if !validate_objects_tx(&mut tx, &scope).await? { return Ok::<_, crate::store::StoreError>(None); }
        configure_selected_node_planning_tx(&mut tx, &scope).await?;
        let ready: bool = sqlx::query_scalar("SELECT library_support_shape($1)")
            .bind(json!([{"kind":"legacy_chunk","graph_id":Uuid::nil(),"atom_id":Uuid::nil(),"projection":"headline_v1","input_sha256":"0".repeat(64)}]))
            .fetch_one(&mut *tx).await?;
        if !ready { return Err(crate::store::StoreError::from(sqlx::Error::Protocol("headline receipt contract unavailable".into()))); }
        let sql = format!("{}{}", scope_cte(&served), RECAP_CANDIDATES_SQL);
        let mut value: Value = sqlx::query_scalar(&sql)
            .bind(&scope.graph_ids).bind(scope.origin_filter()).bind(scope.source_item_id)
            .bind(scope.source_version_id).bind(scope.node_id).bind(scope.collection_key())
            .bind(window.since).bind(window.until).bind(limit)
            .bind(after.as_ref().map(|c| c.event_at)).bind(after.as_ref().map(|c| c.graph_id))
            .bind(after.as_ref().map(|c| c.record_kind.as_str())).bind(after.as_ref().map(|c| c.record_id))
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;
        let last = value.as_object_mut().expect("recap object").remove("_last").unwrap_or(Value::Null);
        value["next_recap_cursor"] = if value["truncated"] == true {
            json!(encode_recap_cursor(
                &scope,user,account,query,window,limit,
                serde_json::from_value(last["event_at"].clone()).map_err(|e|sqlx::Error::Decode(Box::new(e)))?,
                serde_json::from_value(last["graph_id"].clone()).map_err(|e|sqlx::Error::Decode(Box::new(e)))?,
                last["record_kind"].as_str().unwrap_or_default().to_owned(),
                serde_json::from_value(last["record_id"].clone()).map_err(|e|sqlx::Error::Decode(Box::new(e)))?,
            ))
        } else { Value::Null };
        value["scope"] = json!(served);
        value["event_time_window"] = json!({"since":window.since,"until":window.until});
        Ok(Some(value))
    }.await;
    match result {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(failure(
            StatusCode::NOT_FOUND,
            "scope not found or access denied",
        )),
        Err(error) => {
            tracing::warn!(error=%error,"recap retrieval unavailable");
            Err(failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "recap retrieval unavailable",
            ))
        }
    }
}

/// Statements the statement-vector leg admits: the nearest this many
/// permitted, in-scope semantic atoms by their own statement vector
/// (`semantic_atom_embeddings`).
pub const ASK_STATEMENT_ATOMS: i64 = 200;

/// A query vector the vector leg can use: the cloud width, finite, not zero.
/// The stub embedder's zero vector fails this, so dev falls back to lexical.
pub fn usable_query_embedding(v: &[f32]) -> bool {
    v.len() == coralswarm_core::embed::EMBED_DIM
        && v.iter().all(|n| n.is_finite())
        && v.iter().any(|n| *n != 0.0)
}

/// Ask ranking covers the complete admitted set before truncation. Scope
/// admission and capture-version selection remain shared with Library reads.
/// Returned nodes/wider context never enter Ask model evidence.
///
/// Hybrid when `query_embedding` is given: two legs fuse by reciprocal rank.
/// The lexical leg (query terms found in statement + best passage) and the
/// statement-vector leg (cosine of the query to the outcome's own statement
/// vector in `semantic_atom_embeddings`, current statement text only). The
/// statement leg reads vectors stamped
/// [`coralswarm_core::embed::EMBED_MODEL_CLOUD`], and `query_embedding` must
/// come from that same model. Without it the ranking is lexical. Ties break
/// by statement score, lexical score, recency (`occurred_at`), then
/// `semantic_atom_id`, so the order is deterministic. The chunk-vector leg
/// (best chunk `embedding_next` per version) was retired on 2026-10-21: on
/// the 97 gold questions it changed no item hit at 40 and cost 0.5 to 1.3 s.
pub async fn retrieve_ask_candidates(
    tenant: &Tenant,
    user: &UserContext,
    mut scope: LibraryScope,
    terms: &[String],
    query_embedding: Option<&[f32]>,
    event_window: Option<&EventTimeWindow>,
    limit: i64,
) -> Result<Value, Response> {
    if !(1..=200).contains(&limit) || terms.len() > 24
        || terms.iter().any(|t| t.is_empty() || t.chars().count() > 64)
        || query_embedding.is_some_and(|v| !usable_query_embedding(v))
    {
        return Err(failure(StatusCode::BAD_REQUEST, "invalid ask candidates"));
    }
    let account = authorize_scope(tenant, user, &mut scope).await?;
    scope.serve_current();
    let result = async {
        let mut tx = tenant.begin_member_scoped(account).await?;
        crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id).await?;
        sqlx::query(CANDIDATE_TX_SETTINGS_SQL).execute(&mut *tx).await?;
        if !validate_objects_tx(&mut tx, &scope).await? { return Ok::<_, crate::store::StoreError>(None); }
        configure_selected_node_planning_tx(&mut tx, &scope).await?;
        let sql = format!("{}{}{}", scope_cte(&scope), ASK_LEGACY_SCOPE_SQL, ask_candidates_sql());
        let mut value: Value = sqlx::query_scalar(&sql)
            .bind(&scope.graph_ids).bind(scope.origin_filter())
            .bind(scope.source_item_id).bind(scope.source_version_id)
            .bind(scope.node_id).bind(scope.collection_key())
            .bind(terms).bind(query_embedding.map(|v| pgvector::Vector::from(v.to_vec())))
            .bind(limit)
            .bind(event_window.map(|w| w.since)).bind(event_window.map(|w| w.until))
            .bind(coralswarm_core::embed::EMBED_MODEL_CLOUD).bind(ASK_STATEMENT_ATOMS)
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;
        value["scope"] = json!(scope);
        Ok(Some(value))
    }.await;
    match result {
        Ok(Some(v)) => Ok(v),
        Ok(None) => Err(failure(StatusCode::NOT_FOUND,"scope not found or access denied")),
        Err(_) => Err(failure(StatusCode::SERVICE_UNAVAILABLE,"ask retrieval unavailable")),
    }
}

/// Retrieval's legacy selector. `ASK_RECEIPTS_SQL` applies the same
/// predicates per cited atom.
const ASK_LEGACY_SCOPE_SQL: &str = r#"
, ask_unmapped_scope AS MATERIALIZED (SELECT a.graph_id,a.atom_id
 FROM atoms a WHERE a.graph_id=ANY($1) AND library_atom_permitted(a.graph_id,a.atom_id)
 AND NOT a.source_evidence_bound AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id)
 AND $2::jsonb='{}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL
 AND ($5::uuid IS NULL OR EXISTS(SELECT 1 FROM node_atoms l WHERE l.node_id=$5 AND l.atom_id=a.atom_id))
), ask_legacy_scope AS MATERIALIZED (
 SELECT a.* FROM atoms a JOIN ask_unmapped_scope permitted ON permitted.graph_id=a.graph_id AND permitted.atom_id=a.atom_id
 WHERE a.content_inline IS NOT NULL
)
"#;

pub async fn ask_inputs_in_scope_tx(tx: &mut Transaction<'_, Postgres>,scope: &LibraryScope,support: &Value) -> Result<bool,sqlx::Error> {
    let receipts=[(Uuid::nil(),support.clone())];
    Ok(ask_receipts_in_scope_tx(tx,scope,&receipts).await?.first().is_some_and(|(_,allowed)|*allowed))
}

/// Each cited row is looked up on its own against the scope's items
/// (`scope_sources`, `scope_served_versions`), never against a materialised
/// `scope_atoms` or `ask_legacy_scope`: the membership test is the same as
/// theirs, but the whole-scope form cost about a second per fence on the
/// 2026-10-21 corpus and runs four times per Ask. Unreferenced CTEs in
/// `scope_cte` are not evaluated.
const ASK_RECEIPTS_SQL: &str = r#"
 SELECT (r->>'receipt_id')::uuid,NOT EXISTS(SELECT 1 FROM jsonb_array_elements(r->'support') e WHERE
  (e->>'kind'='source_passage' AND NOT EXISTS(SELECT 1 FROM semantic_atoms a
   JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
   JOIN scope_sources s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
   LEFT JOIN scope_served_versions served ON served.graph_id=s.graph_id AND served.source_item_id=s.source_item_id
   WHERE a.graph_id=library_provenance_id(e->>'graph_id') AND a.semantic_atom_id=library_provenance_id(e->>'semantic_atom_id')
    AND a.source_version_id=library_provenance_id(e->>'source_version_id') AND v.source_item_id=library_provenance_id(e->>'source_item_id')
    AND ($4::uuid IS NULL OR a.source_version_id=$4)
    AND ($5::uuid IS NULL OR EXISTS(SELECT 1 FROM node_semantic_atoms l WHERE l.graph_id=a.graph_id AND l.semantic_atom_id=a.semantic_atom_id AND l.node_id=$5))
    AND public.history_floor_admits(v.graph_id,COALESCE(v.occurred_at,v.captured_at),(SELECT public.reader_history_floors()))
    AND __OUTCOME_VERSION_MODE__))
  OR (e->>'kind'='legacy_chunk' AND NOT EXISTS(SELECT 1 FROM atoms a
   WHERE a.graph_id=library_provenance_id(e->>'graph_id') AND a.atom_id=library_provenance_id(e->>'atom_id') AND a.graph_id=ANY($1)
    AND library_atom_permitted(a.graph_id,a.atom_id) AND NOT a.source_evidence_bound
    AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id)
    AND $2::jsonb='{}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL
    AND ($5::uuid IS NULL OR EXISTS(SELECT 1 FROM node_atoms l WHERE l.node_id=$5 AND l.atom_id=a.atom_id))
    AND CASE WHEN NOT(e ? 'projection') THEN a.content_inline IS NOT NULL ELSE e->>'projection'='headline_v1' END)))
 FROM jsonb_array_elements($7::jsonb) r
"#;

pub(crate) async fn ask_receipts_in_scope_tx(
    tx:&mut Transaction<'_,Postgres>,scope:&LibraryScope,receipts:&[(Uuid,Value)],
)->Result<Vec<(Uuid,bool)>,sqlx::Error> {
    sqlx::query("SET LOCAL jit=off").execute(&mut **tx).await?;
    let mode = if reads_served_versions_only(scope) {
        "a.source_version_id=COALESCE(served.version_id,s.current_version_id)"
    } else {
        "true"
    };
    let sql=format!("{}{}",scope_cte(scope),ASK_RECEIPTS_SQL.replace("__OUTCOME_VERSION_MODE__", mode));
    let rows:Vec<Value>=receipts.iter().map(|(id,support)|json!({"receipt_id":id,"support":support})).collect();
    sqlx::query_as(&sql).bind(&scope.graph_ids).bind(scope.origin_filter())
        .bind(scope.source_item_id).bind(scope.source_version_id).bind(scope.node_id).bind(scope.collection_key())
        .bind(json!(rows)).fetch_all(&mut **tx).await
}

/// Live fact supersession (`fact_supersession`): a superseded outcome ranks
/// after current ones and carries `supersession` (history, not current state).
fn ask_candidates_sql() -> String {
    ASK_CANDIDATES_SQL
        .replace("__STATEMENT_WEIGHT__", ASK_STATEMENT_WEIGHT)
        .replace("__SUPERSEDED__", &crate::fact_supersession::superseded_sql("a"))
        .replace("__SUPERSESSION__", &crate::fact_supersession::outcome_supersession_sql("a"))
}

/// Reciprocal-rank weight of the statement leg (lexical 1.0, chunk vector
/// 0.5). Chosen on the 2026-10-20 quality-eval gold with
/// `quality_eval.py retrieval-sim --rank statement` (`backend/eval/QUALITY_EVAL.md`).
const ASK_STATEMENT_WEIGHT: &str = "1.5";

const ASK_CANDIDATES_SQL: &str = r#"
, ask_statement_scored AS MATERIALIZED (
 -- Statement leg: exact cosine of the query to each in-scope statement's
 -- own vector. scope_atoms is already the permitted set (permitted source
 -- items in scope), so an unreadable statement cannot score. A stale vector
 -- (another model, or a statement that changed since) never scores.
 SELECT a.graph_id,a.semantic_atom_id,a.occurred_at,(1-(e.embedding <=> $8))::float8 statement_score
 FROM scope_atoms a JOIN semantic_atom_embeddings e ON e.graph_id=a.graph_id AND e.semantic_atom_id=a.semantic_atom_id
 WHERE $8::vector IS NOT NULL AND e.embedding IS NOT NULL AND e.embed_model=$12
  AND e.statement_sha256=sha256(convert_to(a.statement,'UTF8'))
), ask_statement_top AS MATERIALIZED (
 SELECT * FROM (SELECT s.*,row_number() OVER (ORDER BY s.statement_score DESC,s.occurred_at DESC NULLS LAST,s.graph_id,s.semantic_atom_id) statement_rank
  FROM ask_statement_scored s) ranked WHERE statement_rank<=$13
), ask_lexical AS MATERIALIZED (
 -- Lexical leg: query terms found in the statement plus its best passage.
 -- One join and one aggregate over the scope; no lookup or sort per atom.
 SELECT a.graph_id,a.semantic_atom_id,
  max((SELECT count(*) FROM unnest($7::text[]) t WHERE position(lower(t) IN lower(a.statement||' '||p.quote))>0))::bigint lexical_score
 FROM scope_atoms a JOIN semantic_atom_passages p ON p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id AND p.semantic_atom_id=a.semantic_atom_id
 GROUP BY a.graph_id,a.semantic_atom_id
), ask_matches AS MATERIALIZED (
 SELECT a.*,COALESCE(l.lexical_score,0) lexical_score,st.statement_score,st.statement_rank
 FROM scope_atoms a LEFT JOIN ask_lexical l ON l.graph_id=a.graph_id AND l.semantic_atom_id=a.semantic_atom_id
 LEFT JOIN ask_statement_top st ON st.graph_id=a.graph_id AND st.semantic_atom_id=a.semantic_atom_id
 WHERE ($10::timestamptz IS NULL OR (a.occurred_at >= $10 AND a.occurred_at < $11))
  AND ($10::timestamptz IS NOT NULL OR COALESCE(l.lexical_score,0)>0 OR st.statement_rank IS NOT NULL)
), ask_fused AS MATERIALIZED (
 -- Reciprocal rank fusion. Lexical leg: term coverage, ties by statement
 -- score then recency. Statement leg: the atom's own rank by statement
 -- similarity.
 SELECT m.*,(CASE WHEN m.lexical_score>0 THEN 1.0/(10+row_number() OVER (PARTITION BY m.lexical_score>0
    ORDER BY m.lexical_score DESC,m.statement_score DESC NULLS LAST,m.occurred_at DESC NULLS LAST,m.semantic_atom_id)) ELSE 0 END
  +CASE WHEN m.statement_rank IS NOT NULL THEN __STATEMENT_WEIGHT__/(10+m.statement_rank) ELSE 0 END)::float8 fused_score
 FROM ask_matches m
), ask_leading AS MATERIALIZED (
 -- Supersession is read for the leading rows only; a superseded fact ranks
 -- after the current ones among them.
 SELECT a.*,__SUPERSEDED__ superseded FROM (SELECT * FROM ask_fused
  ORDER BY fused_score DESC,statement_score DESC NULLS LAST,lexical_score DESC,occurred_at DESC NULLS LAST,semantic_atom_id LIMIT GREATEST($9*10,400)) a
), ask_page AS MATERIALIZED (
 -- The cited passage is chosen for the page only: the one with the most
 -- query terms, then the earliest.
 SELECT a.*,p.passage_id,p.start_char,p.end_char,p.quote FROM (SELECT * FROM ask_leading
  ORDER BY superseded,fused_score DESC,statement_score DESC NULLS LAST,lexical_score DESC,occurred_at DESC NULLS LAST,semantic_atom_id LIMIT $9) a
 JOIN LATERAL (
  SELECT p.* FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id
   AND p.source_version_id=a.source_version_id AND p.semantic_atom_id=a.semantic_atom_id
  ORDER BY (SELECT count(*) FROM unnest($7::text[]) t WHERE position(lower(t) IN lower(p.quote))>0) DESC,p.start_char,p.passage_id LIMIT 1
 ) p ON true
), ask_legacy_matches AS MATERIALIZED (
 SELECT a.*,(SELECT count(*) FROM unnest($7::text[]) t WHERE position(lower(t) IN lower(a.content_inline))>0) score
 FROM ask_legacy_scope a WHERE
  ($10::timestamptz IS NOT NULL AND a.timestamp >= $10 AND a.timestamp < $11)
  OR ($10::timestamptz IS NULL AND EXISTS(SELECT 1 FROM unnest($7::text[]) t WHERE position(lower(t) IN lower(a.content_inline))>0))
), ask_legacy_page AS (
 SELECT * FROM ask_legacy_matches ORDER BY score DESC,timestamp DESC,atom_id LIMIT GREATEST(0,$9-(SELECT count(*) FROM ask_page))
)
SELECT jsonb_build_object(
 'outcomes',COALESCE((SELECT jsonb_agg(jsonb_build_object(
  'graph_id',a.graph_id,'semantic_atom_id',a.semantic_atom_id,'kind',a.kind,'statement',substring(a.statement FOR 4000),'statement_truncated',char_length(a.statement)>4000,
  'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,'source_version_is_current',a.source_version_is_current,
  'occurred_at',a.occurred_at,'captured_at',a.captured_at,'representation',a.representation,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,
  'confidence',a.confidence,'synthetic_demo',COALESCE(a.origin->>'transport'='synthetic',false) OR COALESCE(a.source_metadata->'synthetic_demo'='true'::jsonb,false),
  'citations',jsonb_build_array(jsonb_build_object('kind','source_passage','graph_id',a.graph_id,
   'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,'semantic_atom_id',a.semantic_atom_id,'passage_id',a.passage_id,
   'start_char',a.start_char,'end_char',LEAST(a.end_char,a.start_char+4000),'quote',substring(a.quote FOR 4000),'quote_truncated',char_length(a.quote)>4000,
   'representation',a.representation,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,'occurred_at',a.occurred_at,'captured_at',a.captured_at,
   'source_type',a.source_type,'title',a.title)))||__SUPERSESSION__
  ORDER BY a.superseded,a.fused_score DESC,a.statement_score DESC NULLS LAST,a.lexical_score DESC,a.occurred_at DESC NULLS LAST,a.semantic_atom_id) FROM ask_page a),'[]'::jsonb),
 'legacy_evidence',COALESCE((SELECT jsonb_agg(jsonb_build_object('kind','legacy_chunk','graph_id',a.graph_id,'atom_id',a.atom_id,
  'input_sha256',encode(sha256(convert_to(jsonb_build_array(a.content_inline,a.header_len,a.source_type,a.source_id,a.timestamp,a.created_at)::text,'UTF8')),'hex'),
  'representation','unmapped_legacy_chunk','content',substring(a.content_inline FOR 4000),'content_truncated',char_length(a.content_inline)>4000,
  'occurred_at',a.timestamp,'captured_at',a.created_at) ORDER BY a.score DESC,a.timestamp DESC,a.atom_id) FROM ask_legacy_page a),'[]'::jsonb),
 'truncated',(SELECT count(*) FROM ask_matches)+(SELECT count(*) FROM ask_legacy_matches)>$9,
 'reason',CASE WHEN $10::timestamptz IS NOT NULL THEN 'permitted_event_time_evidence' WHEN cardinality($7::text[])=0 AND $8::vector IS NULL THEN 'no_query_terms' ELSE 'ranked_permitted_evidence' END,
 'retrieval',jsonb_build_object('mode',CASE WHEN $8::vector IS NULL THEN 'lexical' ELSE 'hybrid' END,
  'top_statement_score',(SELECT round(max(statement_score)::numeric,4) FROM ask_statement_scored),
  'statement_vectors',(SELECT count(*) FROM ask_statement_scored),
  'top_lexical_score',(SELECT max(lexical_score) FROM ask_page),'query_terms',cardinality($7::text[])))
"#;

const RECAP_CANDIDATES_SQL: &str = r#"
, recap_legacy AS MATERIALIZED (
 SELECT a.graph_id,a.atom_id,a.title,a.title_kind,a.source_type,a.source_id,a.timestamp,a.created_at
 -- history_floor: scoped_permitted_atoms is library_permitted_atoms, which floors.
 FROM atoms a JOIN scoped_permitted_atoms permitted ON permitted.graph_id=a.graph_id AND permitted.atom_id=a.atom_id
 WHERE NOT a.source_evidence_bound AND permitted.unmapped
 AND $2::jsonb='{}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL
 AND ($5::uuid IS NULL OR EXISTS(SELECT 1 FROM node_atoms l WHERE l.node_id=$5 AND l.atom_id=a.atom_id))
 AND a.timestamp >= $7 AND a.timestamp < $8 AND isfinite(a.timestamp) AND isfinite(a.created_at)
), recap_candidates AS MATERIALIZED (
 SELECT a.graph_id,a.semantic_atom_id record_id,a.occurred_at event_at,'retained'::text record_kind
 FROM scope_atoms a WHERE a.occurred_at >= $7 AND a.occurred_at < $8
 AND EXISTS(SELECT 1 FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id AND p.semantic_atom_id=a.semantic_atom_id)
 UNION ALL
 SELECT a.graph_id,a.atom_id,a.timestamp,'legacy_headline'::text FROM recap_legacy a
 WHERE a.title IS NOT NULL AND btrim(a.title)<>'' AND library_legacy_headline_sha256(a.graph_id,a.atom_id) IS NOT NULL
), recap_remaining AS (
 SELECT * FROM recap_candidates WHERE $10::timestamptz IS NULL
 OR (event_at,graph_id,record_kind,record_id)<($10,$11::uuid,$12::text,$13::uuid)
), recap_bounded AS MATERIALIZED (
 SELECT * FROM recap_remaining ORDER BY event_at DESC,graph_id DESC,record_kind DESC,record_id DESC LIMIT ($9::bigint+1)
), recap_page AS MATERIALIZED (
 SELECT * FROM recap_bounded ORDER BY event_at DESC,graph_id DESC,record_kind DESC,record_id DESC LIMIT $9::bigint
), recap_passages AS (
 SELECT a.*,p.passage_id,p.start_char,p.end_char,p.quote FROM scope_atoms a
 JOIN recap_page page ON page.graph_id=a.graph_id AND page.record_kind='retained' AND page.record_id=a.semantic_atom_id
 JOIN LATERAL(SELECT p.* FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.source_version_id=a.source_version_id AND p.semantic_atom_id=a.semantic_atom_id ORDER BY p.start_char,p.passage_id LIMIT 1) p ON true
)
SELECT jsonb_build_object(
 'outcomes',COALESCE((SELECT jsonb_agg(jsonb_build_object(
  'graph_id',a.graph_id,'semantic_atom_id',a.semantic_atom_id,'kind',a.kind,'statement',substring(a.statement FOR 4000),'statement_truncated',char_length(a.statement)>4000,
  'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,'source_version_is_current',a.source_version_is_current,
  'occurred_at',a.occurred_at,'captured_at',a.captured_at,'representation',a.representation,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,
  'confidence',a.confidence,'synthetic_demo',COALESCE(a.origin->>'transport'='synthetic',false) OR COALESCE(a.source_metadata->'synthetic_demo'='true'::jsonb,false),
  'citations',jsonb_build_array(jsonb_build_object('kind','source_passage','graph_id',a.graph_id,
   'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,'semantic_atom_id',a.semantic_atom_id,'passage_id',a.passage_id,
   'start_char',a.start_char,'end_char',LEAST(a.end_char,a.start_char+4000),'quote',substring(a.quote FOR 4000),'quote_truncated',char_length(a.quote)>4000,
   'representation',a.representation,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,
   'occurred_at',a.occurred_at,'captured_at',a.captured_at,'source_type',a.source_type,'title',a.title)))
 ORDER BY a.occurred_at DESC,a.graph_id DESC,a.semantic_atom_id DESC) FROM recap_passages a),'[]'::jsonb),
 'legacy_evidence','[]'::jsonb,
 'legacy_headlines',COALESCE((SELECT jsonb_agg(jsonb_build_object(
  'kind','legacy_headline','representation','unmapped_legacy_headline','graph_id',a.graph_id,'atom_id',a.atom_id,
  'title',substring(a.title FOR 1024),'title_truncated',char_length(a.title)>1024,'title_kind',substring(a.title_kind FOR 128),'title_kind_truncated',COALESCE(char_length(a.title_kind)>128,false),'snippet',substring(a.title FOR 1024),'body_omitted',true,'source_type',substring(a.source_type FOR 256),'source_type_truncated',char_length(a.source_type)>256,
  'occurred_at',a.timestamp,'captured_at',a.created_at,'source_independence','unknown',
  'input_sha256',library_legacy_headline_sha256(a.graph_id,a.atom_id))
 ORDER BY a.timestamp DESC,a.graph_id DESC,a.atom_id DESC) FROM recap_legacy a JOIN recap_page page
 ON page.graph_id=a.graph_id AND page.record_kind='legacy_headline' AND page.record_id=a.atom_id),'[]'::jsonb),
 'untitled_legacy_count',(SELECT count(*) FROM recap_legacy WHERE title IS NULL OR btrim(title)=''),
 'truncated',(SELECT count(*) FROM recap_bounded)>$9,
 '_last',(SELECT to_jsonb(page) FROM recap_page page ORDER BY event_at,graph_id,record_kind,record_id LIMIT 1),
 'reason','permitted_event_time_evidence')
"#;

async fn read_scoped_search(
    tenant: &Tenant,
    user: &UserContext,
    mut q: SearchRequest,
) -> Result<Value, Response> {
    q.query = q.query.trim().to_string();
    if q.query.chars().count() > 200 {
        return Err(failure(StatusCode::BAD_REQUEST, "query too long"));
    }
    let limit = match page_size(q.page_size) {
        Ok(v) => v,
        Err(e) => return Err(failure(StatusCode::BAD_REQUEST, e)),
    };
    let account = match authorize_scope(tenant, user, &mut q.scope).await {
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    let signature = digest(&q.scope, &q.query);
    let sources = match parse_cursor(q.sources_after.as_deref(), "sources", &signature) {
        Ok(v) => v,
        Err(e) => return Err(failure(StatusCode::BAD_REQUEST, e)),
    };
    let outcomes = match parse_cursor(q.outcomes_after.as_deref(), "outcomes", &signature) {
        Ok(v) => v,
        Err(e) => return Err(failure(StatusCode::BAD_REQUEST, e)),
    };
    let nodes = match parse_cursor(q.nodes_after.as_deref(), "nodes", &signature) {
        Ok(v) => v,
        Err(e) => return Err(failure(StatusCode::BAD_REQUEST, e)),
    };
    let mode = read_model::mode();
    let result = async {
        let mut tx = begin_search_tx(tenant, account, mode == read_model::Mode::On).await?;
        if !validate_objects_tx(&mut tx, &q.scope).await? {
            return Ok::<_, crate::store::StoreError>(None);
        }
        let read = read_search_tx(
            &mut tx,
            mode,
            &q.scope,
            &q.query,
            limit,
            sources.as_ref(),
            outcomes.as_ref(),
            nodes.as_ref(),
        )
        .await;
        let (value,decision)=match read {Ok(value)=>value,Err(error)=>{
            let _=rollback_read_model_tx(tx).await;
            return Err(crate::store::StoreError::Db(Box::new(error)));
        }};
        tx.commit().await?;
        if let Some(event)=LiveFallbackEvent::new(tenant,account,&q,&decision) {
            log_live_fallback(&event);
        }
        Ok(Some((value,decision.into_freshness())))
    }
    .await;
    match result {
        Ok(Some((mut value,freshness))) => {
            for group in ["sources", "outcomes", "nodes"] {
                finish_page(&mut value, group, limit as usize, &signature);
            }
            if let Some(work) = value["prominent_work"].as_array_mut() {
                for item in work {
                    if let Some(o) = item.as_object_mut() {
                        for key in ["_id", "_at", "_sources", "_outcomes"] {
                            o.remove(key);
                        }
                    }
                }
            }
            value["scope"] = json!(q.scope);
            // Absent when every component is exact (Live, or an exact
            // Model). Present only while a hub's nodes are served stale.
            if let Some(freshness) = freshness {
                value["freshness"] = freshness;
            }
            Ok(value)
        }
        Ok(None) => Err(failure(
            StatusCode::NOT_FOUND,
            "scope not found or access denied",
        )),
        Err(error) => {
            tracing::warn!(error=%error,"authorized library read failed");
            Err(failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "library read unavailable",
            ))
        }
    }
}

fn finish_page(value: &mut Value, group: &str, limit: usize, signature: &str) {
    let items = value[group]["items"]
        .as_array_mut()
        .expect("database page array");
    let more = items.len() > limit;
    items.truncate(limit);
    let cursor = if more {
        items.last().and_then(|last| {
            let c = Cursor {
                revision: CURSOR_REVISION,
                group: group.to_string(),
                digest: signature.to_string(),
                id: serde_json::from_value(last["_id"].clone()).ok()?,
                at: serde_json::from_value(last["_at"].clone()).ok()?,
                occurred: if group == "outcomes" {
                    serde_json::from_value(last["occurred_at"].clone()).ok()?
                } else {
                    None
                },
                sources: last["_sources"].as_i64(),
                outcomes: last["_outcomes"].as_i64(),
            };
            Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&c).ok()?))
        })
    } else {
        None
    };
    for item in items {
        if let Some(o) = item.as_object_mut() {
            for k in ["_id", "_at", "_sources", "_outcomes"] {
                o.remove(k);
            }
        }
    }
    value[group]["next_cursor"] = json!(cursor);
}

const NODE_PROJECTION: &str = r#"jsonb_build_object(
  'node_id',n.node_id,'graph_id',n.graph_id,'title',library_node_title(n.graph_id,n.node_id),'type',n.type,
  'entity_type',CASE WHEN n.entity_type_sticky THEN n.entity_type ELSE (SELECT c.entity_type FROM page_node_context c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id) END,
  'contribution',jsonb_build_object('source_support_count',n.source_count,'unknown_independence_record_count',n.unknown_independence_record_count,'semantic_atom_count',n.atom_count,'unmapped_legacy_chunk_count',CASE WHEN $2::jsonb='{}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL THEN n.unmapped_legacy_count ELSE 0 END),
  'wider_context',jsonb_build_object(
   'source_support_count',(SELECT c.source_support_count FROM page_node_context c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id),
   'semantic_atom_count',(SELECT c.semantic_atom_count FROM page_node_context c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id),
   'unmapped_legacy_chunk_count',n.unmapped_legacy_count),
  'current_capture',(SELECT jsonb_build_object(
    'source_count',c.capture_source_count,'retained_record_count',c.capture_retained_record_count,'current_version_count',c.capture_current_version_count,
    'unavailable_version_count',c.capture_unavailable_version_count,
    'pending_version_count',c.capture_pending_version_count,
    'incomplete_version_count',c.capture_incomplete_version_count,
    'current_semantic_atom_count',n.current_atom_count,
    'latest_captured_at',c.capture_latest_captured_at,
    'status',c.capture_status)
    FROM page_node_context c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id),
  'latest_captured_at',(SELECT c.latest_captured_at FROM page_node_context c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id),
  'unresolved_disagreement',EXISTS(SELECT 1 FROM assertion_disagreements d WHERE d.graph_id=n.graph_id AND n.node_id IN (d.left_node_id,d.right_node_id) AND d.resolved_at IS NULL),
  'current_understanding_status',CASE WHEN EXISTS(SELECT 1 FROM assertion_disagreements d WHERE d.graph_id=n.graph_id AND n.node_id IN (d.left_node_id,d.right_node_id) AND d.resolved_at IS NULL) THEN 'unresolved_disagreement' ELSE 'evidence_only' END,
  'synthetic_demo',(SELECT c.synthetic_demo FROM page_node_context c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id),
  '_id',n.node_id,'_at',null,'_sources',n.source_count,'_outcomes',n.atom_count
 )"#;

// Fence each version-keyed session result before its NULL check and JSON
// projection reuse; SQL-function inlining otherwise repeats its atom verdict.
pub(crate) const OUTCOME_PROJECTION: &str = r#"jsonb_build_object(
  'graph_id',a.graph_id,'semantic_atom_id',a.semantic_atom_id,'kind',a.kind,'statement',a.statement,'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,
  'origin',a.origin,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,'occurred_at',a.occurred_at,'captured_at',a.captured_at,'source_version_is_current',a.source_version_is_current,
  'nodes',COALESCE((SELECT jsonb_agg(jsonb_build_object('node_id',n.node_id,'title',library_node_title(n.graph_id,n.node_id),'type',n.type) ORDER BY n.node_id)
   FROM (SELECT n.* FROM node_semantic_atoms l JOIN nodes n ON n.node_id=l.node_id AND n.graph_id=l.graph_id WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id AND library_node_permitted(n.graph_id,n.node_id) ORDER BY n.node_id LIMIT 20) n),'[]'::jsonb),
  'citations',COALESCE((SELECT jsonb_agg(jsonb_build_object('kind','source_passage','graph_id',a.graph_id,'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,
   'semantic_atom_id',a.semantic_atom_id,'passage_id',p.passage_id,'start_char',p.start_char,'end_char',LEAST(p.end_char,p.start_char+4000),'quote',substring(p.quote FOR 4000),'quote_truncated',char_length(p.quote)>4000,
   'representation',a.representation,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,'occurred_at',a.occurred_at,'captured_at',a.captured_at) ORDER BY p.start_char,p.passage_id)
   FROM (SELECT p.* FROM semantic_atom_passages p WHERE p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.graph_id=a.graph_id ORDER BY p.start_char,p.passage_id LIMIT 8) p),'[]'::jsonb),
  'nodes_truncated',(SELECT count(*)>20 FROM node_semantic_atoms l WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id AND library_node_permitted(l.graph_id,l.node_id)),
  'citations_truncated',(SELECT count(*)>8 FROM semantic_atom_passages p WHERE p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.graph_id=a.graph_id),
  'session',(SELECT jsonb_build_object('graph_id',a.graph_id,'provider_namespace',sv.provider_namespace,'session_id',sv.session_id) FROM (SELECT sv.* FROM library_session_versions(a.graph_id) sv WHERE sv.version_id=a.source_version_id OFFSET 0) sv WHERE sv.session_id IS NOT NULL),
  '_id',a.semantic_atom_id,'_at',a.captured_at
 )"#;

// Probe the base text columns so selective patterns can use their trigram
// indexes before intersecting with the fully permitted/version-scoped set.
// Include the passage's version as well as its atom/graph, exactly as the
// original correlated predicate did. No text from this candidate set is
// projected; scope_atoms remains the admission boundary.
const INDEXED_OUTCOME_TEXT_CTE: &str = r#"
, text_matched_outcomes AS MATERIALIZED (
 -- history_floor: candidate keys only, intersected with the floored scope_atoms.
 SELECT a.graph_id,a.semantic_atom_id,a.source_version_id FROM semantic_atoms a
 WHERE $7::text IS NOT NULL AND a.graph_id=ANY($1) AND a.statement ILIKE $7
 UNION
 SELECT p.graph_id,p.semantic_atom_id,p.source_version_id FROM semantic_atom_passages p
 WHERE $7::text IS NOT NULL AND p.graph_id=ANY($1) AND p.quote ILIKE $7
)"#;
const INDEXED_OUTCOME_TEXT_PREDICATE: &str =
    "(a.graph_id,a.semantic_atom_id,a.source_version_id) IN (SELECT graph_id,semantic_atom_id,source_version_id FROM text_matched_outcomes)";
// One/two-character searches have no useful trigram. Keep their scan bounded
// to the admitted scope instead of forcing an index or scanning global text.
const SCOPED_OUTCOME_TEXT_PREDICATE: &str = r#"a.statement ILIKE $7
 OR EXISTS(SELECT 1 FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.quote ILIKE $7)"#;

/// The Live search statement. Deprecated with the Live path (stage 1), but
/// the Model builder reuses it as its projection tail and the sealed
/// `model_sql_sha256` covers its text, so it stays unmarked and unchanged
/// until `library_live_fallback` shows Live is no longer served.
const SEARCH_SQL: &str = r#"
__OUTCOME_TEXT_CTE__
, matched_atoms AS MATERIALIZED (
 SELECT a.* FROM scope_atoms a WHERE $7::text IS NULL OR __OUTCOME_TEXT_PREDICATE__
 OR EXISTS(SELECT 1 FROM scope_sources s WHERE s.source_item_id=a.source_item_id AND s.title ILIKE $7)
), matched_sources AS MATERIALIZED (
 -- A source's title/id still match independently. Outcome hits use the
 -- already scoped matched set instead of scanning statement/passages twice.
 -- A title-only outcome hit cannot broaden this set: that same source title
 -- already satisfies the source predicate above.
 SELECT s.* FROM scope_sources s WHERE $7::text IS NULL OR s.title ILIKE $7 OR s.source_id ILIKE $7
 OR EXISTS(SELECT 1 FROM matched_atoms a WHERE a.source_item_id=s.source_item_id)
), title_matched_nodes AS MATERIALIZED (
 -- The scope graphs' nodes whose stored title matches the query, from one
 -- scan (none without a query). Probed per scope node instead, the planner
 -- ran the title function ahead of the probe and resolved every node's
 -- title, three times over (past 300 s on the 2026-10-23 org tenant).
 SELECT n.graph_id,n.node_id FROM nodes n WHERE $7::text IS NOT NULL AND n.graph_id=ANY($1) AND substring(n.title FOR 256) ILIKE $7
), matched_nodes AS MATERIALIZED (
 -- OFFSET 0 fences the membership test ahead of the title function, and the
 -- set is materialised so each matching node's title resolves once, not
 -- once per reader (page, work page and count).
 SELECT n.* FROM (SELECT n.* FROM scope_nodes n WHERE $7::text IS NULL OR (n.graph_id,n.node_id) IN (SELECT t.graph_id,t.node_id FROM title_matched_nodes t) OFFSET 0) n
 WHERE $7::text IS NULL OR library_node_title(n.graph_id,n.node_id) IS NOT NULL
),
source_page AS (
 SELECT s.* FROM matched_sources s WHERE $9::uuid IS NULL
 OR ($8::timestamptz IS NOT NULL AND (s.captured_at<$8 OR (s.captured_at=$8 AND s.source_item_id<$9) OR s.captured_at IS NULL))
 OR ($8::timestamptz IS NULL AND s.captured_at IS NULL AND s.source_item_id<$9)
 ORDER BY s.captured_at DESC NULLS LAST,s.source_item_id DESC LIMIT $15
), atom_page AS (
 SELECT a.* FROM matched_atoms a WHERE $11::uuid IS NULL
 OR ($16::timestamptz IS NOT NULL AND (a.occurred_at<$16 OR (a.occurred_at=$16 AND (a.captured_at,a.semantic_atom_id)<($10,$11)) OR a.occurred_at IS NULL))
 OR ($16::timestamptz IS NULL AND a.occurred_at IS NULL AND (a.captured_at,a.semantic_atom_id)<($10,$11))
 ORDER BY a.occurred_at DESC NULLS LAST,a.captured_at DESC,a.semantic_atom_id DESC LIMIT $15
), atom_page_nodes AS MATERIALIZED (
 -- The page outcomes' linked nodes, each authorised once. Per projected
 -- outcome, the projection authorised and titled the same node again: a
 -- person node of 1.9k outcomes cost 10 ms a title, 60 calls a page, on the
 -- 2026-10-23 org tenant.
 SELECT d.graph_id,d.node_id,n.type FROM (SELECT DISTINCT l.graph_id,l.node_id FROM atom_page a
  JOIN node_semantic_atoms l ON l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id) d
 JOIN nodes n ON n.node_id=d.node_id AND n.graph_id=d.graph_id WHERE library_node_permitted(d.graph_id,d.node_id)
), atom_page_links AS MATERIALIZED (
 -- Each page outcome's permitted links, numbered in node order: the first
 -- twenty are shown, the rest only counted.
 SELECT l.graph_id,l.semantic_atom_id,l.node_id,row_number() OVER (PARTITION BY l.graph_id,l.semantic_atom_id ORDER BY l.node_id) shown
 FROM atom_page a JOIN node_semantic_atoms l ON l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id
 JOIN atom_page_nodes p ON p.graph_id=l.graph_id AND p.node_id=l.node_id
), atom_page_titles AS MATERIALIZED (
 SELECT d.graph_id,d.node_id,library_node_title(d.graph_id,d.node_id) title
 FROM (SELECT DISTINCT graph_id,node_id FROM atom_page_links WHERE shown<=20) d
), node_page AS (
 SELECT n.*,__NODE_COLUMNS__ FROM (SELECT n.* FROM matched_nodes n WHERE $14::uuid IS NULL OR (n.source_count,n.atom_count,n.node_id)<($12,$13,$14)
  ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC LIMIT $15) n JOIN nodes raw ON raw.node_id=n.node_id AND raw.graph_id=n.graph_id
), project_mentions AS MATERIALIZED (
 -- Project mentions of the in-scope outcomes, one row per (outcome, name).
 -- The containment test skips the outcomes with no project entity before
 -- any element is expanded.
 SELECT DISTINCT a.graph_id,a.semantic_atom_id,lower(e->>'mention') mention FROM retained_scope_atoms a
 CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e
 WHERE a.entities @> '[{"type":"project"}]'::jsonb AND e->>'type'='project'
), project_mention_links AS MATERIALIZED (
 -- The mentions carried to their linked nodes by index probe (OFFSET 0
 -- keeps the lateral keyed). Joined to the scope's materialised links
 -- instead, PostgreSQL 16 priced both sets at one row and looped one over
 -- the other (8k by 8k rows, 3.5 s on the p6 facets fixture).
 SELECT DISTINCT l.graph_id,l.node_id,m.mention FROM project_mentions m
 CROSS JOIN LATERAL (SELECT l.graph_id,l.node_id FROM node_semantic_atoms l WHERE l.semantic_atom_id=m.semantic_atom_id AND l.graph_id=m.graph_id AND ($5::uuid IS NULL OR l.node_id=$5) OFFSET 0) l
), project_mention_titles AS MATERIALIZED (
 -- One title read per mentioned node (OFFSET 0 keeps it a keyed lookup: as
 -- a join the planner hashed every nodes row in the tenant, 190k, 450 MB).
 SELECT c.graph_id,c.node_id,n.title FROM (SELECT DISTINCT graph_id,node_id FROM project_mention_links) c
 CROSS JOIN LATERAL (SELECT lower(substring(n.title FOR 256)) title FROM nodes n WHERE n.node_id=c.node_id AND n.graph_id=c.graph_id OFFSET 0) n
), project_mention_nodes AS MATERIALIZED (
 -- Nodes some in-scope outcome names as a project, computed once for the
 -- scope. As a per-node EXISTS inside work_page's OR it ran a scan of the
 -- whole retained_scope_atoms set for every candidate node (an ocean with
 -- no topic nodes: 40k nodes by 27k rows, past 120 s on the 2026-10-23 org
 -- tenant); a materialized set is one hashed membership test per node.
 SELECT DISTINCT p.graph_id,p.node_id FROM project_mention_links p
 JOIN project_mention_titles t ON t.graph_id=p.graph_id AND t.node_id=p.node_id AND t.title=p.mention
), work_page AS (
 -- OFFSET 0 is a planner fence: the inner ORDER BY matches the outer one, so
 -- the node row, the title function and the hashed mention test run on the
 -- ordered candidates only until six titled rows are found (4.4k candidates
 -- and 19 s of title resolution otherwise, 2026-10-23 org tenant).
 SELECT n.*,__NODE_COLUMNS__ FROM (SELECT n.* FROM matched_nodes n WHERE n.source_count>0
  ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC OFFSET 0) n
 JOIN nodes raw ON raw.node_id=n.node_id AND raw.graph_id=n.graph_id
 WHERE (raw.type='topic' OR (raw.entity_type_sticky AND raw.entity_type='project')
  OR (n.graph_id,n.node_id) IN (SELECT p.graph_id,p.node_id FROM project_mention_nodes p))
  AND raw.owner_id IS NULL AND raw.body->>'synthetic_hub' IS DISTINCT FROM 'true'
  AND library_node_title(n.graph_id,n.node_id) IS NOT NULL
 ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC LIMIT 6
), page_nodes AS MATERIALIZED (
 -- The nodes the two node readers project. Every per-node value below is
 -- computed for this set in one pass over the scope-wide set it reads: as a
 -- subquery per projected node, each rescanned a 27k-41k row set (the
 -- 2026-10-23 org tenant: 22 ms a node for the entity type, 8 ms for the
 -- capture counts, 7 ms for the legacy support).
 -- The page set is priced at two rows, so each pass filters its scan by the
 -- page's node ids first (one array): joined as-is, the planner looped
 -- the whole 41k-row node-source set past every page node.
 SELECT graph_id,node_id,entity_type_sticky,raw_title FROM node_page UNION SELECT graph_id,node_id,entity_type_sticky,raw_title FROM work_page
), page_legacy AS MATERIALIZED (
 SELECT x.graph_id,x.node_id,array_agg(DISTINCT x.source_item_id) FILTER (WHERE x.independent) independent_items,bool_or(x.synthetic) synthetic
 FROM scoped_legacy_links x JOIN page_nodes p ON p.graph_id=x.graph_id AND p.node_id=x.node_id
 WHERE x.node_id=ANY(ARRAY(SELECT node_id FROM page_nodes)) GROUP BY 1,2
), page_capture AS MATERIALIZED (
 SELECT links.graph_id,links.node_id,
  count(*) FILTER(WHERE NOT COALESCE(s.is_legacy_baseline,false) AND s.source_metadata->>'independence' IS DISTINCT FROM 'derived_copy') source_count,
  count(*) retained_record_count,count(*) FILTER(WHERE s.current_version_id IS NOT NULL) current_version_count,
  count(*) FILTER(WHERE s.current_version_id IS NULL) unavailable_version_count,
  count(*) FILTER(WHERE s.current_extraction_status IN('not_started','pending','running')) pending_version_count,
  count(*) FILTER(WHERE s.current_extraction_status IN('failed','blocked')) incomplete_version_count,
  count(*) FILTER(WHERE s.current_version_id IS NULL OR s.current_extraction_status IN('failed','blocked')) incomplete_or_unavailable_count,
  max(s.current_captured_at) latest_captured_at
 FROM scoped_node_sources links JOIN page_nodes p ON p.graph_id=links.graph_id AND p.node_id=links.node_id
 JOIN scope_sources s ON s.graph_id=links.graph_id AND s.source_item_id=links.source_item_id
 WHERE links.node_id=ANY(ARRAY(SELECT node_id FROM page_nodes))
 GROUP BY 1,2
), page_entity_types AS MATERIALIZED (
 -- For a page node whose entity type is not sticky, the type under which
 -- its latest in-scope outcome mentions the node's title.
 -- OFFSET 0 fences the mention expansion so the page set joins it last: as
 -- one join the planner looped the page nodes outermost and rescanned the
 -- retained outcomes per node.
 SELECT DISTINCT ON (c.graph_id,c.node_id) c.graph_id,c.node_id,c.entity_type
 FROM (SELECT l.graph_id,l.node_id,lower(e->>'mention') mention,e->>'type' entity_type,a.captured_at,a.semantic_atom_id
  FROM scoped_semantic_links l JOIN retained_scope_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e
  WHERE l.node_id=ANY(ARRAY(SELECT node_id FROM page_nodes WHERE NOT entity_type_sticky)) OFFSET 0) c
 JOIN page_nodes p ON p.graph_id=c.graph_id AND p.node_id=c.node_id AND lower(p.raw_title)=c.mention
 ORDER BY c.graph_id,c.node_id,c.captured_at DESC,c.semantic_atom_id DESC
), page_node_context AS MATERIALIZED (
 -- The page nodes' wider-context fields from one walk of each node's links
 -- (every retained outcome of a permitted item, the scope's filters aside),
 -- with the page sets above joined by key. A node with no scoped source
 -- carries zero capture counts and the 'unavailable' status.
 SELECT p.graph_id,p.node_id,w.semantic_atom_count,w.latest_captured_at,
  (SELECT count(DISTINCT u.source_item_id) FROM (SELECT unnest(w.independent_items) source_item_id UNION SELECT unnest(x.independent_items)) u) source_support_count,
  w.synthetic OR COALESCE(x.synthetic,false) synthetic_demo,et.entity_type,
  COALESCE(c.source_count,0) capture_source_count,COALESCE(c.retained_record_count,0) capture_retained_record_count,
  COALESCE(c.current_version_count,0) capture_current_version_count,COALESCE(c.unavailable_version_count,0) capture_unavailable_version_count,
  COALESCE(c.pending_version_count,0) capture_pending_version_count,COALESCE(c.incomplete_version_count,0) capture_incomplete_version_count,
  c.latest_captured_at capture_latest_captured_at,
  CASE WHEN COALESCE(c.current_version_count,0)=0 THEN 'unavailable' WHEN c.incomplete_or_unavailable_count>0 THEN 'incomplete'
   WHEN c.pending_version_count>0 THEN 'pending' ELSE 'ready' END capture_status
 FROM page_nodes p
 LEFT JOIN page_legacy x ON x.graph_id=p.graph_id AND x.node_id=p.node_id
 LEFT JOIN page_capture c ON c.graph_id=p.graph_id AND c.node_id=p.node_id
 LEFT JOIN page_entity_types et ON et.graph_id=p.graph_id AND et.node_id=p.node_id
 CROSS JOIN LATERAL (
  SELECT count(*) semantic_atom_count,max(v.captured_at) latest_captured_at,
   COALESCE(bool_or(v.origin->>'transport'='synthetic' OR v.source_metadata->'synthetic_demo'='true'::jsonb),false) synthetic,
   array_agg(DISTINCT v.source_item_id) FILTER (WHERE NOT v.is_legacy_baseline AND v.source_metadata->>'independence' IS DISTINCT FROM 'derived_copy') independent_items
  FROM node_semantic_atoms l JOIN semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
  JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
  JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
  WHERE l.node_id=p.node_id AND l.graph_id=p.graph_id AND s.access_state='permitted' AND public.history_floor_admits(v.graph_id,COALESCE(v.occurred_at,v.captured_at),(SELECT public.reader_history_floors()))) w
), source_item_counts AS MATERIALIZED (
 -- Per-item evidence counts for the source page, one pass each. Correlated
 -- counts rescanned scope_atoms and scoped_chunk_atoms per page row (30 ms a
 -- row on the 2026-10-23 org tenant, 1.2 s a page).
 SELECT s.graph_id,s.source_item_id,COALESCE(a.atom_count,0) atom_count,COALESCE(x.legacy_chunk_count,0) legacy_chunk_count
 FROM source_page s
 LEFT JOIN (SELECT a.source_item_id,count(*) atom_count FROM scope_atoms a GROUP BY 1) a ON a.source_item_id=s.source_item_id
 LEFT JOIN (SELECT x.graph_id,x.source_item_id,count(DISTINCT x.atom_id) legacy_chunk_count FROM scoped_chunk_atoms x
  WHERE $4::uuid IS NULL OR x.version_id=$4 GROUP BY 1,2) x ON x.graph_id=s.graph_id AND x.source_item_id=s.source_item_id
), service_facets AS (SELECT origin->>'service_id' value,count(*) source_count FROM matched_sources WHERE origin->>'service_id' IS NOT NULL GROUP BY 1 ORDER BY 1),
kind_facets AS (SELECT discovery_content_kind value,count(*) source_count FROM matched_sources WHERE discovery_content_kind IS NOT NULL GROUP BY 1 ORDER BY 1),
connection_facets AS (SELECT jsonb_build_object('service_id',origin->'service_id','connection',origin->'connection') value,count(*) source_count FROM matched_sources WHERE jsonb_typeof(origin->'connection')='object' GROUP BY 1 ORDER BY 1),
container_facets AS (SELECT jsonb_build_object('service_id',origin->'service_id','container',(origin->'container')-'label') value,max(origin->'container'->>'label') label,count(*) source_count FROM matched_sources WHERE jsonb_typeof(origin->'container')='object' GROUP BY 1 ORDER BY 1)
SELECT jsonb_build_object(
 'sources',jsonb_build_object('total',(SELECT count(*) FROM matched_sources),'items',COALESCE((SELECT jsonb_agg(jsonb_build_object(
  'graph_id',s.graph_id,'source_item_id',s.source_item_id,'source_type',s.source_type,'provider_namespace',s.provider_namespace,'source_id',s.source_id,
  'title',s.title,'current_version_id',s.current_version_id,'selected_version_id',s.selected_version_id,'origin',s.origin,'version_origin',s.version_origin,
  'representation',s.representation,'source_independence',CASE WHEN COALESCE(s.is_legacy_baseline,false) OR s.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,'occurred_at',s.occurred_at,'captured_at',s.captured_at,'updated_at',s.updated_at,'current_captured_at',s.current_captured_at,'current_extraction_status',s.current_extraction_status,'selected_extraction_status',s.selected_extraction_status,'selected_extraction_reason',s.extraction_error,'current_extraction_reason',s.current_extraction_error,
  'semantic_atom_count',COALESCE((SELECT c.atom_count FROM source_item_counts c WHERE c.source_item_id=s.source_item_id),0),
  'legacy_chunk_count',COALESCE((SELECT c.legacy_chunk_count FROM source_item_counts c WHERE c.graph_id=s.graph_id AND c.source_item_id=s.source_item_id),0),
  'synthetic_demo',COALESCE(s.origin->>'transport'='synthetic',false) OR COALESCE(s.source_metadata->'synthetic_demo'='true'::jsonb,false),
  'session',(SELECT jsonb_build_object('graph_id',s.graph_id,'provider_namespace',sv.provider_namespace,'session_id',sv.session_id) FROM (SELECT sv.* FROM library_session_versions(s.graph_id) sv WHERE sv.version_id=s.selected_version_id OFFSET 0) sv WHERE sv.session_id IS NOT NULL),
  '_id',s.source_item_id,'_at',s.captured_at
 ) ORDER BY s.captured_at DESC NULLS LAST,s.source_item_id DESC) FROM source_page s),'[]'::jsonb)),
 'outcomes',jsonb_build_object('total',(SELECT count(*) FROM matched_atoms),'items',COALESCE((SELECT jsonb_agg(__OUTCOME_PROJECTION__ ORDER BY a.occurred_at DESC NULLS LAST,a.captured_at DESC,a.semantic_atom_id DESC) FROM atom_page a),'[]'::jsonb)),
 'nodes',jsonb_build_object('total',(SELECT count(*) FROM matched_nodes),'items',COALESCE((SELECT jsonb_agg(__NODE_PROJECTION__ ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC) FROM node_page n),'[]'::jsonb)),
 'prominent_work',COALESCE((SELECT jsonb_agg(__NODE_PROJECTION__ ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC) FROM work_page n),'[]'::jsonb),
 'facets',jsonb_build_object(
  'services',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value,'value',value,'label',null,'source_count',source_count)) FROM (SELECT * FROM service_facets LIMIT 100) f),'[]'::jsonb),
  'content_kinds',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value,'value',value,'label',null,'source_count',source_count)) FROM (SELECT * FROM kind_facets LIMIT 100) f),'[]'::jsonb),
  'connections',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value::text,'value',value,'label',null,'source_count',source_count)) FROM (SELECT * FROM connection_facets LIMIT 100) f),'[]'::jsonb),
  'containers',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value::text,'value',value,'label',label,'source_count',source_count)) FROM (SELECT * FROM container_facets LIMIT 100) f),'[]'::jsonb),
  'unknown_origin_count',(SELECT count(*) FROM matched_sources WHERE origin IS NULL),
  'truncated',(SELECT count(*)>100 FROM service_facets) OR (SELECT count(*)>100 FROM kind_facets) OR (SELECT count(*)>100 FROM connection_facets) OR (SELECT count(*)>100 FROM container_facets)))
"#;

/// A page node's own columns, from the `nodes` row (`raw`) its page joins.
const NODE_COLUMNS_SQL: &str =
    "raw.type,raw.entity_type,raw.entity_type_sticky,substring(raw.title FOR 256) raw_title";

/// `OUTCOME_PROJECTION`'s two per-row node entries, verbatim, and the page-set
/// forms the search read substitutes for them (`atom_page_nodes`,
/// `atom_page_links` and `atom_page_titles` in `SEARCH_SQL`). The other
/// readers of the projection show one outcome or a short list and keep the
/// per-row form; `outcome_nodes_are_read_from_the_page_sets` proves the
/// substitution still matches.
const OUTCOME_NODES_PER_ROW: &str = r#"'nodes',COALESCE((SELECT jsonb_agg(jsonb_build_object('node_id',n.node_id,'title',library_node_title(n.graph_id,n.node_id),'type',n.type) ORDER BY n.node_id)
   FROM (SELECT n.* FROM node_semantic_atoms l JOIN nodes n ON n.node_id=l.node_id AND n.graph_id=l.graph_id WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id AND library_node_permitted(n.graph_id,n.node_id) ORDER BY n.node_id LIMIT 20) n),'[]'::jsonb),"#;
const OUTCOME_NODES_PAGE: &str = r#"'nodes',COALESCE((SELECT jsonb_agg(jsonb_build_object('node_id',l.node_id,'title',t.title,'type',p.type) ORDER BY l.node_id)
   FROM atom_page_links l JOIN atom_page_nodes p ON p.graph_id=l.graph_id AND p.node_id=l.node_id JOIN atom_page_titles t ON t.graph_id=l.graph_id AND t.node_id=l.node_id
   WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id AND l.shown<=20),'[]'::jsonb),"#;
const OUTCOME_NODES_TRUNCATED_PER_ROW: &str = r#"'nodes_truncated',(SELECT count(*)>20 FROM node_semantic_atoms l WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id AND library_node_permitted(l.graph_id,l.node_id)),"#;
const OUTCOME_NODES_TRUNCATED_PAGE: &str = r#"'nodes_truncated',(SELECT count(*)>20 FROM atom_page_links l WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id),"#;

fn search_outcome_projection() -> String {
    OUTCOME_PROJECTION
        .replace(OUTCOME_NODES_PER_ROW, OUTCOME_NODES_PAGE)
        .replace(OUTCOME_NODES_TRUNCATED_PER_ROW, OUTCOME_NODES_TRUNCATED_PAGE)
}

#[cfg(any(test, feature = "test-support"))]
fn search_sql(scope: &LibraryScope) -> String {search_sql_with_query(scope,"indexed")}

fn render_search_projection(sql:String,scope:&LibraryScope,query:&str)->String {
    // Preserve PR5's actual short/long and exact-scope strategy in both
    // independent readers; every text/identity value remains a bound input.
    let indexed=query.chars().count()>=3 && scope.source_item_id.is_none()
        && scope.source_version_id.is_none() && scope.node_id.is_none();
    sql.replace("__OUTCOME_TEXT_CTE__",if indexed {INDEXED_OUTCOME_TEXT_CTE}else{""})
        .replace("__OUTCOME_TEXT_PREDICATE__",if indexed {INDEXED_OUTCOME_TEXT_PREDICATE}else{SCOPED_OUTCOME_TEXT_PREDICATE})
        .replace("__NODE_PROJECTION__",NODE_PROJECTION)
        .replace("__NODE_COLUMNS__",NODE_COLUMNS_SQL)
        .replace("__OUTCOME_PROJECTION__",&format!("({}||{})",search_outcome_projection(),crate::fact_supersession::outcome_supersession_sql("a")))
}
/// Renders the Live statement. Deprecated with the Live path (stage 1); not
/// marked `#[deprecated]` because the Model program's conditional Live
/// fallback renders it too. `library_live_fallback` decides stage 2.
fn search_sql_with_query(scope:&LibraryScope,query:&str)->String {
    render_search_projection(format!("{}{SEARCH_SQL}",scope_cte(scope)),scope,query)
}
/// The Live statement for EXPLAIN probes. Deprecated with the Live path
/// (stage 1); removed with it once `library_live_fallback` reaches zero.
#[cfg(feature = "test-support")]
pub fn search_sql_for_explain(scope: &LibraryScope) -> String {
    search_sql(scope)
}

/// The actual short/long-pattern branch, for equality and EXPLAIN probes.
/// Deprecated with the Live path (stage 1); removed with it once
/// `library_live_fallback` reaches zero.
#[cfg(feature = "test-support")]
pub fn search_sql_for_text_explain(scope: &LibraryScope, query: &str) -> String {
    search_sql_with_query(scope, query)
}

#[cfg(feature = "test-support")]
pub fn read_model_sql_for_test(scope: &LibraryScope) -> String {
    read_model::sql(scope,"")
}

#[cfg(feature = "test-support")]
pub fn scope_sql_for_explain(scope: &LibraryScope) -> String {
    scope_cte(scope)
}

async fn read_search_tx(
    tx: &mut Transaction<'_, Postgres>, mode: read_model::Mode, scope: &LibraryScope,
    query: &str, limit: i64, sources: Option<&Cursor>, outcomes: Option<&Cursor>,nodes: Option<&Cursor>,
) -> Result<(Value,PathDecision), CandidateProgramError> {
    match mode {
        read_model::Mode::On=>{
            let result=candidate_program::execute(tx,candidate_program::Arguments{scope,query,limit,sources,outcomes,nodes},std::time::Duration::from_millis(read_model::PROGRAM_WALL_MILLIS)).await?;
            log_read_freshness(scope,result.freshness.as_ref(),result.backlog_oldest_at.as_deref());
            let decision=if result.path=="model" {PathDecision::Model{freshness:result.freshness}}
                else {PathDecision::LiveProgram{reason:result.reason.unwrap_or_default()}};
            Ok((result.value,decision))
        }
        read_model::Mode::Off=>{
            #[allow(deprecated)]
            let value=live_search_tx(tx,scope,query,limit,sources,outcomes,nodes).await?;
            Ok((value,PathDecision::LiveKillSwitch))
        }
    }
}

#[deprecated(note="Library Live path, stage 1: serve through the Model program; every call logs library_live_fallback")]
async fn live_search_tx(
    tx: &mut Transaction<'_, Postgres>, scope: &LibraryScope,
    query: &str, limit: i64, sources: Option<&Cursor>, outcomes: Option<&Cursor>, nodes: Option<&Cursor>,
) -> Result<Value, sqlx::Error> {
    read_search_statement_tx(tx,&search_sql_with_query(scope,query),scope,query,limit,sources,outcomes,nodes).await
}

/// Structured events for a stale serve (`library_read_stale_serve`, one per
/// read served with stale nodes) and for a change backlog past the alarm
/// ceiling (`library_change_backlog_alarm`). Counts and ages only: no graph,
/// item or node identity, title, or query text.
fn log_read_freshness(scope:&LibraryScope,freshness:Option<&Value>,backlog_oldest_at:Option<&str>) {
    let now=chrono::Utc::now();
    let age_ms=|at:Option<&str>|at.and_then(|t|chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t|(now-t.with_timezone(&chrono::Utc)).num_milliseconds().max(0));
    let stale_ms=freshness.and_then(|f|age_ms(f["stale_since"].as_str()));
    let backlog_ms=age_ms(backlog_oldest_at).filter(|ms|*ms>read_model::CHANGE_AGE_ALARM_SECS*1000);
    if freshness.is_none() && backlog_ms.is_none() {return}
    // The JSON formatter falls back to the current (request) span even for a
    // root event. Enter a field-free root so no ambient request context is
    // serialized; skip only these events if that root is disabled.
    let root=tracing::info_span!(parent: None, "library_read_freshness");
    if root.is_disabled() {return}
    root.in_scope(|| {
        if let Some(freshness)=freshness {
            tracing::info!(parent: &root,event="library_read_stale_serve",components=%freshness["components"],
                stale_ms=stale_ms.unwrap_or(-1),stale_limit_secs=read_model::HUB_STALE_SECS,graphs=scope.graph_ids.len(),
                "Library read served stale nodes while a hub change drains");
        }
        if let Some(ms)=backlog_ms {
            tracing::warn!(parent: &root,event="library_change_backlog_alarm",oldest_age_ms=ms,alarm_secs=read_model::CHANGE_AGE_ALARM_SECS,
                graphs=scope.graph_ids.len(),"Library change backlog older than the alarm ceiling");
        }
    });
}

/// Executes a search statement with the sixteen request binds. Serves the
/// deprecated Live statement and the Model statements alike, so it stays
/// unmarked; Live serves through it are counted by `library_live_fallback`.
async fn read_search_statement_tx(
    tx: &mut Transaction<'_, Postgres>, sql: &str, scope: &LibraryScope,
    query: &str, limit: i64, sources: Option<&Cursor>, outcomes: Option<&Cursor>, nodes: Option<&Cursor>,
) -> Result<Value, sqlx::Error> {
    sqlx::query_scalar(sql)

        .bind(&scope.graph_ids)
        .bind(scope.origin_filter())
        .bind(scope.source_item_id)
        .bind(scope.source_version_id)
        .bind(scope.node_id)
        .bind(scope.collection_key())
        .bind(pattern(query))
        .bind(sources.and_then(|c| c.at))
        .bind(sources.map(|c| c.id))
        .bind(outcomes.and_then(|c| c.at))
        .bind(outcomes.map(|c| c.id))
        .bind(nodes.and_then(|c| c.sources))
        .bind(nodes.and_then(|c| c.outcomes))
        .bind(nodes.map(|c| c.id))
        .bind(limit + 1)
        .bind(outcomes.and_then(|c| c.occurred))
        .fetch_one(&mut **tx)
        .await
}

/// Bounded acknowledged transaction cleanup shared by native/background
/// proofs. Its one-second allowance is separate from the candidate wall.
pub async fn rollback_read_model_tx(tx:Transaction<'_,Postgres>)->Result<(),CandidateProgramError> {
    candidate_program::cleanup(tx.rollback()).await
}

/// Proof failures preserve which reader failed without exposing SQL or data.
#[derive(Debug, thiserror::Error)]
pub enum ModelComparisonError {
    #[error("Library proof context failed")]
    Context(#[from] sqlx::Error),
    #[error("Library proof live reader failed")]
    Live(sqlx::Error),
    #[error("Library proof candidate reader failed")]
    Candidate(CandidateProgramError),
}

/// The components a stale serve keeps exact: everything but the node groups
/// (`nodes`, `prominent_work`), which a hub serves from the stored rows.
pub fn exact_components(value: &Value) -> Value {
    let mut value = value.clone();
    if let Some(o) = value.as_object_mut() {
        o.remove("nodes");
        o.remove("prominent_work");
    }
    value
}

/// Both readers' raw ordered JSON plus path classification, the pg suites'
/// same-snapshot Model-equals-Live oracle. Callers never persist these values.
pub struct ModelComparison {
    pub live: Value,
    pub candidate: Value,
    pub path: String,
    pub fallback_reason: Option<String>,
    /// The candidate's `freshness` marker: present only for a Model value
    /// served with stale nodes (a hub). Never part of `candidate`.
    pub freshness: Option<Value>,
}

/// Test seam using the real readers and cursor parser, under the served
/// program wall. Requires a caller-scoped read-only RR transaction opened
/// BEFORE GUC reads. A live fallback is classified separately and cannot
/// prove model equality.
pub async fn compare_read_model_tx(
    tx: &mut Transaction<'_, Postgres>, scope: &LibraryScope, query: &str,
    limit: i64, after: [Option<&str>; 3],
) -> Result<ModelComparison, ModelComparisonError> {
    let budget=std::time::Duration::from_millis(read_model::PROGRAM_WALL_MILLIS);
    let (isolation, read_only, jit): (String,String,String) = sqlx::query_as(
        "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),current_setting('jit')"
    ).fetch_one(&mut **tx).await?;
    if isolation != "repeatable read" || read_only != "on" {
        return Err(sqlx::Error::Protocol("Library comparison requires read-only REPEATABLE READ".into()).into());
    }
    // Every serving and proof caller applies SEARCH_TX_SETTINGS_SQL first. With
    // jit on, the dirty attempt's plan (root cost about 717k before statistics
    // land) crosses jit_optimize_above_cost, and the LLVM compile alone took
    // 5.2 to 7.9 s on PG16 and PG17, most of the 12 s program wall. That is
    // how the moved-atom proof went red on 2026-10-26 (a test opened the
    // transaction without the bundle). Refuse that configuration here rather
    // than letting it look like a slow candidate.
    if jit != "off" {
        return Err(sqlx::Error::Protocol("Library comparison requires the served search settings (jit off)".into()).into());
    }
    let signature=digest(scope,query);
    let sources=parse_cursor(after[0],"sources",&signature).map_err(|_|sqlx::Error::Protocol("invalid proof source cursor".into()))?;
    let outcomes=parse_cursor(after[1],"outcomes",&signature).map_err(|_|sqlx::Error::Protocol("invalid proof outcome cursor".into()))?;
    let nodes=parse_cursor(after[2],"nodes",&signature).map_err(|_|sqlx::Error::Protocol("invalid proof node cursor".into()))?;
    let mut live=read_search_statement_tx(tx,&search_sql_with_query(scope,query),scope,query,limit,sources.as_ref(),outcomes.as_ref(),nodes.as_ref()).await.map_err(ModelComparisonError::Live)?;
    let result=candidate_program::execute(tx,candidate_program::Arguments{scope,query,limit,sources:sources.as_ref(),outcomes:outcomes.as_ref(),nodes:nodes.as_ref()},budget).await.map_err(ModelComparisonError::Candidate)?;
    let mut candidate=result.value;
    // Compare the same public pages/cursors, including nulls and ordering.
    for value in [&mut live,&mut candidate] {
        for group in ["sources","outcomes","nodes"] { finish_page(value,group,limit as usize,&signature); }
        if let Some(work)=value["prominent_work"].as_array_mut() {
            for item in work { if let Some(row)=item.as_object_mut() {
                for key in ["_id","_at","_sources","_outcomes"] { row.remove(key); }
            }}
        }
        value["scope"]=json!(scope);
    }
    Ok(ModelComparison {live,candidate,path:result.path.to_owned(),fallback_reason:result.reason,freshness:result.freshness})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeRequest {
    pub scope: LibraryScope,
}
/// Authorize and validate a scope, then echo it: `{scope}` and nothing else.
///
/// Until 2026-10-22 this also returned `source_count`, `outcome_count` and
/// `node_count` from a full `scope_cte` evaluation. No client read them, and
/// `scope_nodes` joins every permitted chunk atom in the scope to
/// `node_atoms` by atom alone: on the 2026-10-22 org tenant that was 1.0M
/// link rows and 4.0 s for one 2.9k-item workspace (with `scope_atoms` at
/// 1.0 s, the 5 s budget), and 56M rows and 114 s for the largest, so every
/// chat's scope check answered 503. Totals for a scope are the search
/// reader's, which pages them; Ask's candidate read never touches
/// `scope_nodes`.
pub async fn resolve_scope(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(mut q): LibraryJson<ScopeRequest>,
) -> Response {
    let account = match authorize_scope(&tenant, &user, &mut q.scope).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let result=async{
        let mut tx=tenant.begin_member_scoped(account).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        if !validate_objects_tx(&mut tx,&q.scope).await?{return Ok::<_,crate::store::StoreError>(None)}
        tx.commit().await?;Ok(Some(()))
    }.await;
    match result {
        Ok(Some(())) => response(StatusCode::OK, json!({"scope": q.scope})),
        Ok(None) => failure(StatusCode::NOT_FOUND, "scope not found or access denied"),
        Err(error) => {
            tracing::warn!(error=%error,"authorized library read failed");
            failure(StatusCode::SERVICE_UNAVAILABLE, "library read unavailable")
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeighborhoodRequest {
    pub scope: LibraryScope,
    pub node_id: Uuid,
    pub page_size: Option<i64>,
    pub after: Option<String>,
}
pub async fn neighborhood(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(mut q): LibraryJson<NeighborhoodRequest>,
) -> Response {
    if q.scope.node_id.is_some_and(|id| id != q.node_id) {
        return failure(StatusCode::BAD_REQUEST, "conflicting node scope");
    }
    q.scope.node_id = Some(q.node_id);
    let limit = match page_size(q.page_size) {
        Ok(v) => v,
        Err(e) => return failure(StatusCode::BAD_REQUEST, e),
    };
    let account = match authorize_scope(&tenant, &user, &mut q.scope).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let signature = digest(&q.scope, "neighborhood");
    let after = match parse_cursor(q.after.as_deref(), "neighbors", &signature) {
        Ok(v) => v,
        Err(e) => return failure(StatusCode::BAD_REQUEST, e),
    };
    let result=async{
        let mut tx=tenant.begin_member_scoped(account).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        // The lineage laterals inflate the plan cost past jit_above_cost, and JIT
        // compilation took 1.8 s of a 2.1 s hub read on PG16; the read itself is 0.3 s.
        sqlx::query("SET LOCAL jit=off").execute(&mut *tx).await?;
        if !validate_objects_tx(&mut tx,&q.scope).await?{return Ok::<_,crate::store::StoreError>(None)}
        let value = neighborhood_page_tx(&mut tx, &q.scope, q.node_id, limit, after, signature).await?;
        tx.commit().await?;
        Ok(Some(value))
    }.await;
    match result {
        Ok(Some(v)) => response(StatusCode::OK, v),
        Ok(None) => failure(StatusCode::NOT_FOUND, "scope not found or access denied"),
        Err(error) => {
            tracing::warn!(error=%error,"authorized library read failed");
            failure(StatusCode::SERVICE_UNAVAILABLE, "library read unavailable")
        }
    }
}

/// Real support only: `scope_node_items` holds every version supporting $5
/// through semantic atoms or permission-checked legacy chunks, and the join to
/// `scope_sources` applies source admission and the item/version/origin/collection
/// selectors. Retained support counts, as the node's source support count does.
/// The key is the history resolver's own attribution of one permitted version,
/// so every row opens through `/library/history`. A version belongs to one
/// session, so (last_captured_at, latest_version_id) is a unique keyset per row.
const NODE_SESSIONS_SQL: &str = r#"
, node_session_versions AS MATERIALIZED (
 SELECT i.graph_id,i.version_id,sv.provider_namespace,sv.session_id,sv.captured_at
 FROM scope_node_items i
 JOIN scope_sources s ON s.graph_id=i.graph_id AND s.source_item_id=i.source_item_id
 CROSS JOIN LATERAL (SELECT sv.* FROM library_session_versions(i.graph_id) sv WHERE sv.version_id=i.version_id) sv
 WHERE ($4::uuid IS NULL OR i.version_id=$4) AND sv.session_id IS NOT NULL
  -- The reader's history floor per version (#1545): a legacy chunk's mapped
  -- version is not floored by scope_node_items.
  AND EXISTS(SELECT 1 FROM source_versions fv WHERE fv.graph_id=i.graph_id AND fv.version_id=i.version_id AND __VERSION_FLOOR__)
), node_sessions AS MATERIALIZED (
 SELECT graph_id,provider_namespace,session_id,count(*) version_count,max(captured_at) last_captured_at,
  (array_agg(version_id ORDER BY captured_at DESC,version_id DESC))[1] latest_version_id
 FROM node_session_versions GROUP BY 1,2,3
), page AS (
 SELECT * FROM node_sessions WHERE $7::uuid IS NULL OR (last_captured_at,latest_version_id)<($8,$7)
 ORDER BY last_captured_at DESC,latest_version_id DESC LIMIT $9
)
SELECT jsonb_build_object('total',(SELECT count(*) FROM node_sessions),'items',COALESCE((SELECT jsonb_agg(jsonb_build_object(
 'graph_id',p.graph_id,'provider_namespace',p.provider_namespace,'session_id',p.session_id,
 'title',(SELECT substring(COALESCE(v.capture_context->>'session_title',v.source_metadata->>'session_title') FOR 512) FROM source_versions v WHERE v.graph_id=p.graph_id AND v.version_id=p.latest_version_id),
 'version_count',p.version_count,'last_captured_at',p.last_captured_at,'latest_version_id',p.latest_version_id)
 ORDER BY p.last_captured_at DESC,p.latest_version_id DESC) FROM page p),'[]'::jsonb))
"#;
fn node_sessions_sql(scope: &LibraryScope) -> String {
    format!("{}{}", scope_cte(scope), NODE_SESSIONS_SQL.replace("__VERSION_FLOOR__", &crate::history_floor::admits_version("fv")))
}
#[cfg(feature = "test-support")]
pub fn node_sessions_sql_for_explain(scope: &LibraryScope) -> String {
    node_sessions_sql(scope)
}

/// Sessions whose retained versions support `node_id` in the selected scope,
/// newest supporting capture first, bounded and keyset paged.
pub async fn node_sessions(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(mut q): LibraryJson<NeighborhoodRequest>,
) -> Response {
    if q.scope.node_id.is_some_and(|id| id != q.node_id) {
        return failure(StatusCode::BAD_REQUEST, "conflicting node scope");
    }
    q.scope.node_id = Some(q.node_id);
    let limit = match page_size(q.page_size) {
        Ok(v) => v,
        Err(e) => return failure(StatusCode::BAD_REQUEST, e),
    };
    let account = match authorize_scope(&tenant, &user, &mut q.scope).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let signature = digest(&q.scope, "node_sessions");
    let after = match parse_cursor(q.after.as_deref(), "node_sessions", &signature) {
        Ok(v) => v,
        Err(e) => return failure(StatusCode::BAD_REQUEST, e),
    };
    let result = async {
        let mut tx = tenant.begin_member_scoped(account).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        sqlx::query("SET LOCAL jit=off").execute(&mut *tx).await?;
        if !validate_objects_tx(&mut tx, &q.scope).await? {
            return Ok::<_, crate::store::StoreError>(None);
        }
        let value = node_sessions_page_tx(&mut tx, &q.scope, q.node_id, limit, after, signature).await?;
        tx.commit().await?;
        Ok(Some(value))
    }
    .await;
    match result {
        Ok(Some(v)) => response(StatusCode::OK, v),
        Ok(None) => failure(StatusCode::NOT_FOUND, "scope not found or access denied"),
        Err(error) => {
            tracing::warn!(error=%error,"authorized library read failed");
            failure(StatusCode::SERVICE_UNAVAILABLE, "library read unavailable")
        }
    }
}

/// Complete option discovery complements the bounded first-100 search summary.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FacetDimension {
    Services,
    ContentKinds,
    Connections,
    Containers,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FacetRequest {
    pub scope: LibraryScope,
    pub dimension: FacetDimension,
    #[serde(default)]
    pub option_query: String,
    pub page_size: Option<i64>,
    pub after: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FacetCursor {
    revision: u8,
    digest: String,
    key: String,
}

impl FacetDimension {
    fn projection(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Services=>("origin->'service_id'","NULL::text","origin->>'service_id' IS NOT NULL"),
            Self::ContentKinds=>("to_jsonb(discovery_content_kind)","NULL::text","discovery_content_kind IS NOT NULL"),
            Self::Connections=>("jsonb_build_object('service_id',origin->'service_id','connection',origin->'connection')","NULL::text","jsonb_typeof(origin->'connection')='object'"),
            Self::Containers=>("jsonb_build_object('service_id',origin->'service_id','container',(origin->'container')-'label')","max(origin->'container'->>'label')","jsonb_typeof(origin->'container')='object'"),
        }
    }
    fn search_text(self) -> &'static str {
        match self {
            Self::Services=>"value #>> '{}'",
            Self::ContentKinds=>"value #>> '{}'",
            Self::Connections=>"concat_ws(' ',value->>'service_id',value->'connection'->>'kind',value->'connection'->>'id')",
            Self::Containers=>"concat_ws(' ',value->>'service_id',value->'container'->>'kind',value->'container'->>'id',value->'container'->>'provider_namespace',label)",
        }
    }
}

pub async fn facets(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(mut q): LibraryJson<FacetRequest>,
) -> Response {
    q.option_query = q.option_query.trim().to_string();
    if q.option_query.chars().count() > 100 {
        return failure(StatusCode::BAD_REQUEST, "option query too long");
    }
    let limit = q.page_size.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return failure(StatusCode::BAD_REQUEST, "invalid facet page size");
    }
    let account = match authorize_scope(&tenant, &user, &mut q.scope).await {
        Ok(account) => account,
        Err(response) => return response,
    };
    let signature = crate::services::source_reingest::content_hash_hex(
        &json!({"scope":q.scope,"dimension":q.dimension,"option_query":q.option_query}).to_string(),
    );
    let after = match q.after.as_deref() {
        None => None,
        Some(value) => {
            let cursor: Option<FacetCursor> = if value.len() <= 16384 {
                URL_SAFE_NO_PAD
                    .decode(value)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            } else {
                None
            };
            match cursor {
                Some(c) if c.revision == 1 && c.digest == signature && c.key.len() <= 8192 => {
                    Some(c.key)
                }
                _ => {
                    return failure(
                        StatusCode::BAD_REQUEST,
                        "facet cursor does not match request",
                    )
                }
            }
        }
    };
    let result=async {
        let mut tx=tenant.begin_member_scoped(account).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        if !validate_objects_tx(&mut tx,&q.scope).await? {return Ok::<_,crate::store::StoreError>(None)}
        let (projection,label,condition)=q.dimension.projection();
        let search=q.dimension.search_text();
        let cte=scope_cte(&q.scope);
        // Every substituted SQL fragment is an enum-selected static literal.
        // User scope, option text and cursor values remain bound parameters.
        let sql=format!(r#"{cte}, options AS MATERIALIZED (
         SELECT {projection} value,{label} label,count(*) source_count FROM scope_sources
         WHERE {condition} GROUP BY 1
        ), matched AS MATERIALIZED (
         SELECT value,label,source_count,CASE WHEN jsonb_typeof(value)='string' THEN value #>> '{{}}' ELSE value::text END key
         FROM options WHERE $7::text IS NULL OR {search} ILIKE $7
        ), page AS (
         SELECT * FROM matched WHERE $8::text IS NULL OR key COLLATE "C">$8 COLLATE "C"
         ORDER BY key COLLATE "C" LIMIT $9
        ) SELECT jsonb_build_object('total',(SELECT count(*) FROM matched),
         'items',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',key,'value',value,'label',label,'source_count',source_count) ORDER BY key COLLATE "C") FROM page),'[]'::jsonb))"#);
        let value:Value=sqlx::query_scalar(&sql).bind(&q.scope.graph_ids).bind(q.scope.origin_filter()).bind(q.scope.source_item_id)
            .bind(q.scope.source_version_id).bind(q.scope.node_id).bind(q.scope.collection_key()).bind(pattern(&q.option_query)).bind(after).bind(limit+1)
            .fetch_one(&mut *tx).await?;
        tx.commit().await?;Ok(Some(value))
    }.await;
    match result {
        Ok(Some(mut value)) => {
            let items = value["items"].as_array_mut().expect("database facet array");
            let more = items.len() > limit as usize;
            items.truncate(limit as usize);
            let cursor = if more {
                items
                    .last()
                    .and_then(|last| last["key"].as_str())
                    .map(|key| {
                        URL_SAFE_NO_PAD.encode(
                            serde_json::to_vec(&FacetCursor {
                                revision: 1,
                                digest: signature,
                                key: key.to_string(),
                            })
                            .expect("facet cursor serialization"),
                        )
                    })
            } else {
                None
            };
            value["scope"] = json!(q.scope);
            value["dimension"] = json!(q.dimension);
            value["next_cursor"] = json!(cursor);
            value["truncated"] = json!(more);
            response(StatusCode::OK, value)
        }
        Ok(None) => failure(StatusCode::NOT_FOUND, "scope not found or access denied"),
        Err(error) => {
            tracing::warn!(error=%error,"authorized library facet discovery failed");
            failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "library facets unavailable",
            )
        }
    }
}

#[cfg(test)]
mod search_projection_tests {
    use super::*;

    /// The search read swaps `OUTCOME_PROJECTION`'s per-row node entries for
    /// the page sets. Both substrings must still match the projection, or
    /// the swap silently does nothing and every outcome authorises and
    /// titles its nodes again.
    #[test]
    fn outcome_nodes_are_read_from_the_page_sets() {
        assert!(OUTCOME_PROJECTION.contains(OUTCOME_NODES_PER_ROW));
        assert!(OUTCOME_PROJECTION.contains(OUTCOME_NODES_TRUNCATED_PER_ROW));
        let projection = search_outcome_projection();
        assert!(!projection.contains(OUTCOME_NODES_PER_ROW));
        assert!(!projection.contains(OUTCOME_NODES_TRUNCATED_PER_ROW));
        assert!(!projection.contains("library_node_permitted("));
        assert!(!projection.contains("library_node_title("));
        assert!(projection.contains("atom_page_titles t"));
        let mut scope = LibraryScope {
            graph_ids: vec![Uuid::nil()],
            outcome_versions: Some(OutcomeVersions::AllRetained),
            service_id: None,
            connection: None,
            container: None,
            content_kind: None,
            collection: None,
            source_item_id: None,
            source_version_id: None,
            node_id: None,
        };
        scope.normalize().unwrap();
        let sql = search_sql(&scope);
        assert!(sql.contains("atom_page_titles AS MATERIALIZED"));
        assert!(!sql.contains(OUTCOME_NODES_PER_ROW));
    }
}

/// Connector activity must be executed in the caller's member-scoped transaction.
/// Parameters are account, capturing user, registered source types and readable graph IDs. Admission
/// precedes every projection, count and topic join, including legacy atoms.
pub(crate) const CONNECTOR_ACTIVITY_CTE: &str = r#"WITH admitted AS MATERIALIZED (
 SELECT a.* FROM graphs g
 CROSS JOIN LATERAL library_permitted_atoms(g.id,NULL) permitted
 JOIN atoms a ON a.atom_id=permitted.atom_id AND a.graph_id=g.id
 WHERE g.account_id=$1 AND g.id=ANY($4::uuid[]) AND a.ingested_by=$2 AND (a.source_type=ANY($3) OR EXISTS (
 SELECT 1 FROM unnest($3::text[]) t WHERE split_part(t,'_',1) IN ('jira','linear','confluence','hubspot','zendesk','dart')
 AND starts_with(a.source_type,split_part(t,'_',1)||'_')))
) "#;

/// Resolve the same graph reachability as Library authorization before activity
/// admission. Account scoping alone is insufficient on the owner-backed tenant plane.
pub(crate) async fn connector_activity_graphs(tenant: &Tenant, user: &UserContext) -> Result<Vec<Uuid>, crate::store::StoreError> {
    let store = tenant.store.as_ref().ok_or_else(|| crate::store::StoreError::Db(Box::from("activity store unavailable")))?;
    crate::tenancy::reachability::ReachabilityResolver::readable_graph_ids(tenant.decision.as_ref(),store.as_ref(),user.user_id).await
}


async fn neighborhood_page_tx(
    tx: &mut Transaction<'_,Postgres>, scope: &LibraryScope, node: Uuid,
    limit: i64, after: Option<Cursor>, signature: String,
) -> Result<Value, crate::store::StoreError> {
        let cte=scope_cte(scope);
        // `valid_from` / `valid_until` are the edge's stated validity (extraction
        // v2, #1339): RFC 3339 instants in UTC, null when the source stated none.
        // v2 anchors a stated calendar date at UTC midnight, so the UTC date is
        // the stated day; `valid_until` is the stated end as written.
        // Edges stream newest first and LIMIT stops the walk once the page fills.
        // OFFSET 0, LIMIT 1 and the CASEs keep permission checks out of table
        // scans: each runs only on a keyed row (a next lineage hop, a supported edge).
        // A NULL atom list means every atom to library_permitted_atoms, so leaves map it
        // to '{}'. That uses CASE because RLS treats COALESCE as leaky, which turned the
        // atoms_pkey lookup into a scan of the graph's atoms (8.4 s on the hub fixture).
        // An exact source selector excludes unmapped legacy refs. Resolve its
        // mapped atom IDs once through the same authoritative admission helper,
        // rather than invoking that helper for every rejected edge lineage.
        let (source_atoms, leaves) = if scope.source_item_id.is_some() {
            (r#", neighborhood_source_atom_ids AS MATERIALIZED (
             SELECT c.graph_id,array_agg(DISTINCT c.atom_id) atom_ids
             FROM scope_sources s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id
             JOIN source_version_chunks c ON c.graph_id=v.graph_id AND c.source_version_id=v.version_id
             WHERE ($4::uuid IS NULL OR v.version_id=$4) GROUP BY c.graph_id
            ), neighborhood_source_atoms AS MATERIALIZED (
             SELECT ids.graph_id,p.atom_id FROM neighborhood_source_atom_ids ids
             CROSS JOIN LATERAL library_permitted_atoms(ids.graph_id,ids.atom_ids) p
            )"#,
            r#"SELECT DISTINCT a.atom_id FROM lineage l JOIN neighborhood_source_atoms a
             ON a.graph_id=e.graph_id AND a.atom_id=ANY(library_provenance_ids(l.provenance->'atom_ids'))"#)
        } else {
            ("", r#"SELECT DISTINCT a.atom_id FROM lineage l
             CROSS JOIN LATERAL library_permitted_atoms(e.graph_id,CASE WHEN library_provenance_ids(l.provenance->'atom_ids') IS NULL THEN '{}' ELSE library_provenance_ids(l.provenance->'atom_ids') END) a"#)
        };
        let sql=format!(r#"{cte}{source_atoms}
        SELECT jsonb_build_object('edge_id',e.edge_id,'predicate',e.predicate,'origin',e.origin,'status',e.status,'confidence',e.confidence,
         'reviewed_by',CASE WHEN e.provenance->>'reviewed_by' ~* '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$' THEN e.provenance->'reviewed_by' ELSE null END,
         'src',jsonb_build_object('node_id',src.node_id,'title',library_node_title(src.graph_id,src.node_id),'type',src.type),
         'dst',jsonb_build_object('node_id',dst.node_id,'title',library_node_title(dst.graph_id,dst.node_id),'type',dst.type),
         'support',support.items,'support_truncated',support.total>64,
         'valid_from',to_char(e.valid_from AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),
         'valid_until',to_char(e.valid_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'))
        FROM (SELECT * FROM relationship_edge e WHERE e.graph_id=ANY($1) AND (e.src_node_id=$5 OR e.dst_node_id=$5) AND ($7::uuid IS NULL OR e.edge_id<$7)
         ORDER BY e.edge_id DESC OFFSET 0) e
        JOIN nodes src ON src.node_id=e.src_node_id AND src.graph_id=e.graph_id
        JOIN nodes dst ON dst.node_id=e.dst_node_id AND dst.graph_id=e.graph_id
        CROSS JOIN LATERAL (
         WITH RECURSIVE lineage AS (
          SELECT e.edge_id id,e.provenance,ARRAY[e.edge_id] seen,0 depth
          UNION ALL
          SELECT p.edge_id,p.provenance,l.seen||p.edge_id,l.depth+1 FROM lineage l
          CROSS JOIN LATERAL unnest(library_provenance_ids(CASE WHEN jsonb_typeof(l.provenance->'source_edge_ids')='array' THEN CASE WHEN jsonb_array_length(l.provenance->'source_edge_ids')<=32 THEN l.provenance->'source_edge_ids' END END)) ids(id)
          CROSS JOIN LATERAL (SELECT * FROM relationship_edge p WHERE p.graph_id=e.graph_id AND p.edge_id=ids.id LIMIT 1) p
          WHERE CASE WHEN l.depth<8 AND NOT p.edge_id=ANY(l.seen) AND p.status='confirmed' THEN library_edge_permitted(e.graph_id,p.edge_id) ELSE false END
         ), leaves AS (
          {leaves}
         ), refs AS (
          SELECT DISTINCT jsonb_build_object('source_item_id',s.source_item_id,'source_version_id',v.version_id,'source_version_is_current',COALESCE(v.version_id=s.current_version_id,false)) ref
          FROM leaves a JOIN source_version_chunks c ON c.atom_id=a.atom_id AND c.graph_id=e.graph_id
          JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
          JOIN scope_sources s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
          WHERE ($4::uuid IS NULL OR v.version_id=$4)
          UNION
          SELECT jsonb_build_object('kind','legacy_chunk','graph_id',e.graph_id,'atom_id',a.atom_id) FROM leaves a
          WHERE $2::jsonb='{{}}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL
           AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=e.graph_id AND c.atom_id=a.atom_id)
         ) SELECT (SELECT count(*) FROM refs) total,COALESCE((SELECT jsonb_agg(ref ORDER BY ref) FROM (SELECT ref FROM refs ORDER BY ref LIMIT 64) p),'[]'::jsonb) items
        ) support
        WHERE CASE WHEN support.total>0 THEN library_node_permitted(e.graph_id,src.node_id) AND library_node_permitted(e.graph_id,dst.node_id)
          AND library_edge_permitted(e.graph_id,e.edge_id) ELSE false END
        ORDER BY e.edge_id DESC LIMIT $8"#);
        let mut items:Vec<Value>=sqlx::query_scalar(&sql).bind(&scope.graph_ids).bind(scope.origin_filter()).bind(scope.source_item_id)
            .bind(scope.source_version_id).bind(scope.node_id).bind(scope.collection_key()).bind(after.as_ref().map(|c|c.id)).bind(limit+1).fetch_all(&mut **tx).await?;
        let more=items.len()>limit as usize;items.truncate(limit as usize);
        let cursor=if more{items.last().and_then(|v|{
            let id=serde_json::from_value(v["edge_id"].clone()).ok()?;
            Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor{revision:CURSOR_REVISION,group:"neighbors".into(),digest:signature.clone(),id,at:None,occurred:None,sources:None,outcomes:None}).ok()?))
        })}else{None};
        Ok(json!({"scope":scope,"node_id":node,"items":items,"next_cursor":cursor,"truncated":more}))
}

pub(crate) async fn neighborhood_first_tx(tx: &mut Transaction<'_,Postgres>, scope: &LibraryScope, node: Uuid) -> Result<Value, crate::store::StoreError> {
    neighborhood_page_tx(tx,scope,node,20,None,digest(scope,"neighborhood" )).await
}


async fn node_sessions_page_tx(
    tx: &mut Transaction<'_,Postgres>, scope: &LibraryScope, node: Uuid,
    limit: i64, after: Option<Cursor>, signature: String,
) -> Result<Value, crate::store::StoreError> {
        let value: Value = sqlx::query_scalar(&node_sessions_sql(scope))
            .bind(&scope.graph_ids)
            .bind(scope.origin_filter())
            .bind(scope.source_item_id)
            .bind(scope.source_version_id)
            .bind(scope.node_id)
            .bind(scope.collection_key())
            .bind(after.as_ref().map(|c| c.id))
            .bind(after.as_ref().and_then(|c| c.at))
            .bind(limit + 1)
            .fetch_one(&mut **tx)
            .await?;
        let mut v = value;
            let items = v["items"].as_array_mut().expect("database page array");
            let more = items.len() > limit as usize;
            items.truncate(limit as usize);
            let cursor = if more {
                items.last().and_then(|i| {
                    let c = Cursor {
                        revision: CURSOR_REVISION,
                        group: "node_sessions".into(),
                        digest: signature.clone(),
                        id: serde_json::from_value(i["latest_version_id"].clone()).ok()?,
                        at: Some(serde_json::from_value(i["last_captured_at"].clone()).ok()?),
                        occurred: None,
                        sources: None,
                        outcomes: None,
                    };
                    Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&c).ok()?))
                })
            } else {
                None
            };
            v["scope"] = json!(scope);
            v["node_id"] = json!(node);
            v["next_cursor"] = json!(cursor);
            v["truncated"] = json!(more);
        Ok(v)
}

pub(crate) async fn node_sessions_first_tx(tx: &mut Transaction<'_,Postgres>, scope: &LibraryScope, node: Uuid) -> Result<Value, crate::store::StoreError> {
    node_sessions_page_tx(tx,scope,node,20,None,digest(scope,"node_sessions" )).await
}

/// Authoritative source SELECT for a page-model worker. Never reads a child
/// payload or opens another snapshot; interactive search keeps its own routing.
pub(crate) async fn page_search_tx(tx: &mut Transaction<'_,Postgres>, scope: &LibraryScope) -> Result<Value, sqlx::Error> {
    let mut value=read_search_statement_tx(tx, &search_sql_with_query(scope,""),scope,"",20,None,None,None).await?;
    let signature=digest(scope,"");
    for group in ["sources","outcomes","nodes"] {finish_page(&mut value,group,20,&signature);}
    if let Some(work)=value["prominent_work"].as_array_mut(){for item in work{if let Some(o)=item.as_object_mut(){for key in ["_id","_at","_sources","_outcomes"]{o.remove(key);}}}}
    value["scope"]=json!(scope);Ok(value)
}
