//! Read-only sides of the unresolved disagreements around one readable node.
//! An opposing side the caller cannot read never becomes a row: SQL counts it
//! and emits nothing else, so no type here can carry its id, title or evidence.
use crate::{
    library_read::{self, LibraryJson, LibraryScope, OUTCOME_PROJECTION},
    middleware::auth::UserContext,
    store::StoreError,
    tenancy::extractor::Tenant,
};
use axum::{
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisagreementRequest {
    pub graph_id: Uuid,
    pub node_id: Uuid,
    pub page_size: Option<i64>,
    pub after: Option<String>,
}

/// A page checks permission on each pair it walks until `size + 1` readable
/// sides are found, then projects at most three outcomes per side.
#[derive(Clone, Copy)]
struct PageSize(i64);
impl TryFrom<Option<i64>> for PageSize {
    type Error = &'static str;
    fn try_from(value: Option<i64>) -> Result<Self, Self::Error> {
        let n = value.unwrap_or(5);
        if (1..=10).contains(&n) {
            Ok(Self(n))
        } else {
            Err("invalid page size")
        }
    }
}

/// Client-visible. It holds only values of an emitted, permitted pair, which
/// the client has already seen; the digest binds it to (graph, node).
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DisagreementCursor {
    revision: u8,
    digest: String,
    recorded_at: DateTime<Utc>,
    other: Uuid,
}
const CURSOR_REVISION: u8 = 1;
fn cursor_digest(graph: Uuid, node: Uuid) -> String {
    crate::services::source_reingest::content_hash_hex(
        &json!({"graph_id":graph,"node_id":node,"read":"disagreements"}).to_string(),
    )
}
fn parse_cursor(raw: Option<&str>, digest: &str) -> Result<Option<DisagreementCursor>, &'static str> {
    let Some(raw) = raw else { return Ok(None) };
    let cursor: DisagreementCursor = (raw.len() <= 2048)
        .then(|| URL_SAFE_NO_PAD.decode(raw).ok())
        .flatten()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or("malformed cursor")?;
    if cursor.revision != CURSOR_REVISION || cursor.digest != digest {
        return Err("cursor does not match request");
    }
    Ok(Some(cursor))
}

// SQL builds these objects; deserializing them here pins the wire contract,
// so a projection drift fails the pg suite instead of reaching the client.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Side {
    node_id: Uuid,
    r#type: String,
    title: Option<String>,
    evidence: Vec<SideEvidence>,
    evidence_truncated: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SideEvidence {
    /// OUTCOME_PROJECTION output without its internal paging keys.
    outcome: Value,
    /// First observation of the outcome's source version. Never a capture date.
    knowledge_time: Option<DateTime<Utc>>,
    /// Supported temporal records for (side node, outcome atom). Empty means
    /// not recorded.
    effective_time: Vec<EffectiveInterval>,
    effective_time_truncated: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectiveInterval {
    state_key: String,
    /// A withdrawal or correction is not an ordinary interval; the reader must see which.
    operation: String,
    value: Option<String>,
    valid_from: Option<DateTime<Utc>>,
    valid_until: Option<DateTime<Utc>>,
    recorded_at: DateTime<Utc>,
    passage_id: Uuid,
}
#[derive(Serialize)]
struct Disagreement {
    recorded_at: DateTime<Utc>,
    other: Side,
}
#[derive(Serialize)]
struct Withheld {
    count: i64,
    complete: bool,
}
#[derive(Serialize)]
struct DisagreementPage {
    graph_id: Uuid,
    node_id: Uuid,
    anchor: Side,
    items: Vec<Disagreement>,
    withheld: Withheld,
    next_cursor: Option<String>,
    truncated: bool,
}

const PAIRS: &str = r#"pairs AS (
 SELECT d.created_at recorded_at,d.right_node_id other FROM assertion_disagreements d
  WHERE d.graph_id=$1 AND d.left_node_id=$2 AND d.resolved_at IS NULL
 UNION ALL
 SELECT d.created_at,d.left_node_id FROM assertion_disagreements d
  WHERE d.graph_id=$1 AND d.right_node_id=$2 AND d.resolved_at IS NULL
)"#;

/// The column set OUTCOME_PROJECTION reads from `a`, matching
/// `retained_scope_atoms` in the Library scope CTE but keyed on one node.
const PERMITTED_NODE_ATOMS: &str = r#"SELECT a.*,v.origin,v.occurred_at,v.captured_at,v.representation,v.source_metadata,v.is_legacy_baseline
 FROM node_semantic_atoms l
 JOIN served_semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
 JOIN source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
 WHERE l.graph_id=n.graph_id AND l.node_id=n.node_id
 ORDER BY a.source_version_is_current DESC,v.captured_at DESC,a.semantic_atom_id DESC"#;

/// A temporal record's passage belongs to the outcome's own version, so the
/// permitted-source gate on the outcome already covers it.
const SUPPORTED_TEMPORAL: &str = r#"FROM temporal_evidence_records e
 WHERE e.graph_id=a.graph_id AND e.subject_node_id=n.node_id AND e.semantic_atom_id=a.semantic_atom_id AND e.lifecycle='supported'"#;

const SIDE_PROJECTION: &str = r#"jsonb_build_object('node_id',n.node_id,'type',n.type,'title',library_node_title(n.graph_id,n.node_id),
 'evidence',COALESCE((SELECT jsonb_agg(jsonb_build_object(
   'outcome',__OUTCOME_PROJECTION__,
   'knowledge_time',(SELECT min(o.known_at) FROM source_version_observations o WHERE o.graph_id=a.graph_id AND o.source_version_id=a.source_version_id),
   'effective_time',COALESCE((SELECT jsonb_agg(jsonb_build_object('state_key',e.state_key,'operation',e.operation,'value',e.value,'valid_from',e.valid_from,'valid_until',e.valid_until,'recorded_at',e.known_at,'passage_id',e.passage_id) ORDER BY e.known_at DESC,e.event_id DESC)
    FROM (SELECT e.* __SUPPORTED_TEMPORAL__ ORDER BY e.known_at DESC,e.event_id DESC LIMIT 4) e),'[]'::jsonb),
   'effective_time_truncated',(SELECT count(*)>4 FROM (SELECT 1 __SUPPORTED_TEMPORAL__ LIMIT 5) x))
  ORDER BY a.source_version_is_current DESC,a.captured_at DESC,a.semantic_atom_id DESC)
  FROM (__PERMITTED_NODE_ATOMS__ LIMIT 3) a),'[]'::jsonb),
 'evidence_truncated',(SELECT count(*)>3 FROM (__PERMITTED_NODE_ATOMS__ LIMIT 4) x))"#;

fn side_projection() -> String {
    SIDE_PROJECTION
        .replace("__OUTCOME_PROJECTION__", OUTCOME_PROJECTION)
        .replace("__SUPPORTED_TEMPORAL__", SUPPORTED_TEMPORAL)
        .replace("__PERMITTED_NODE_ATOMS__", PERMITTED_NODE_ATOMS)
}

/// Withheld pairs are probed over the most recent unresolved pairs only, so
/// the permission work per request stays bounded.
const WITHHELD_PROBE: i64 = 200;

fn reply(status: StatusCode, value: Value) -> Response {
    (status, [(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
}
fn fail(status: StatusCode, message: &str) -> Response {
    reply(status, json!({"error":message}))
}

pub async fn disagreements(
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    LibraryJson(q): LibraryJson<DisagreementRequest>,
) -> Response {
    let size = match PageSize::try_from(q.page_size) {
        Ok(size) => size,
        Err(e) => return fail(StatusCode::BAD_REQUEST, e),
    };
    let digest = cursor_digest(q.graph_id, q.node_id);
    let after = match parse_cursor(q.after.as_deref(), &digest) {
        Ok(after) => after,
        Err(e) => return fail(StatusCode::BAD_REQUEST, e),
    };
    let mut scope = LibraryScope::exact_node(q.graph_id, q.node_id);
    let account = match library_read::authorize_scope(&tenant, &user, &mut scope).await {
        Ok(account) => account,
        Err(response) => return response,
    };
    let result = async {
        let mut tx = tenant.begin_member_scoped(account).await?;
        crate::tenancy::scope::set_tenant_user(&mut tx, user.user_id).await?;
        sqlx::query("SET LOCAL statement_timeout='5s'").execute(&mut *tx).await?;
        // OUTCOME_PROJECTION's per-node laterals push plan cost past jit_above_cost.
        sqlx::query("SET LOCAL jit=off").execute(&mut *tx).await?;
        if !library_read::validate_objects_tx(&mut tx, &scope).await? {
            return Ok::<_, StoreError>(None);
        }
        let page = read_page_tx(&mut tx, q.graph_id, q.node_id, size, after.as_ref(), &digest).await?;
        tx.commit().await?;
        Ok(Some(page))
    }
    .await;
    match result {
        Ok(Some(page)) => reply(StatusCode::OK, json!(page)),
        Ok(None) => fail(StatusCode::NOT_FOUND, "scope not found or access denied"),
        Err(error) => {
            tracing::warn!(error=%error, "library disagreement read failed");
            fail(StatusCode::SERVICE_UNAVAILABLE, "library read unavailable")
        }
    }
}

fn side(mut value: Value) -> Result<Side, StoreError> {
    if let Some(evidence) = value["evidence"].as_array_mut() {
        for item in evidence {
            if let Some(outcome) = item["outcome"].as_object_mut() {
                outcome.remove("_id");
                outcome.remove("_at");
            }
        }
    }
    serde_json::from_value(value).map_err(|e| StoreError::Db(Box::new(e)))
}

async fn read_page_tx(
    tx: &mut Transaction<'static, Postgres>,
    graph: Uuid,
    node: Uuid,
    size: PageSize,
    after: Option<&DisagreementCursor>,
    digest: &str,
) -> Result<DisagreementPage, StoreError> {
    let projection = side_projection();
    let anchor: Value = sqlx::query_scalar(&format!(
        "SELECT {projection} FROM nodes n WHERE n.graph_id=$1 AND n.node_id=$2"
    ))
    .bind(graph)
    .bind(node)
    .fetch_one(&mut **tx)
    .await?;
    // OFFSET 0 keeps the permission call off the pair scan: pairs stream newest
    // first, each walked pair is checked once, and LIMIT stops the walk.
    let rows: Vec<(DateTime<Utc>, Value)> = sqlx::query_as(&format!(
        r#"WITH {PAIRS}, page AS (
         SELECT p.recorded_at,p.other FROM (SELECT * FROM pairs WHERE $3::timestamptz IS NULL OR (recorded_at,other)<($3,$4::uuid)
          ORDER BY recorded_at DESC,other DESC OFFSET 0) p
         WHERE library_node_permitted($1,p.other)
         ORDER BY p.recorded_at DESC,p.other DESC LIMIT $5)
        SELECT p.recorded_at,{projection} FROM page p JOIN nodes n ON n.graph_id=$1 AND n.node_id=p.other
        ORDER BY p.recorded_at DESC,p.other DESC"#
    ))
    .bind(graph)
    .bind(node)
    .bind(after.map(|c| c.recorded_at))
    .bind(after.map(|c| c.other))
    .bind(size.0 + 1)
    .fetch_all(&mut **tx)
    .await?;
    let (withheld, probed): (i64, i64) = sqlx::query_as(&format!(
        r#"WITH {PAIRS}, probe AS (
         SELECT other,row_number() OVER (ORDER BY recorded_at DESC,other DESC) rn FROM pairs
         ORDER BY recorded_at DESC,other DESC LIMIT $3+1)
        SELECT count(*) FILTER (WHERE rn<=$3 AND NOT library_node_permitted($1,other)),count(*) FROM probe"#
    ))
    .bind(graph)
    .bind(node)
    .bind(WITHHELD_PROBE)
    .fetch_one(&mut **tx)
    .await?;
    let more = rows.len() > size.0 as usize;
    let items = rows
        .into_iter()
        .take(size.0 as usize)
        .map(|(recorded_at, other)| Ok(Disagreement { recorded_at, other: side(other)? }))
        .collect::<Result<Vec<_>, StoreError>>()?;
    let next_cursor = more
        .then(|| items.last())
        .flatten()
        .map(|last| {
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&DisagreementCursor {
                    revision: CURSOR_REVISION,
                    digest: digest.to_string(),
                    recorded_at: last.recorded_at,
                    other: last.other.node_id,
                })
                .expect("cursor serializes"),
            )
        });
    Ok(DisagreementPage {
        graph_id: graph,
        node_id: node,
        anchor: side(anchor)?,
        items,
        withheld: Withheld { count: withheld, complete: probed <= WITHHELD_PROBE },
        next_cursor,
        truncated: more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_defaults_to_five_and_refuses_outside_one_to_ten() {
        assert_eq!(PageSize::try_from(None).map(|s| s.0), Ok(5));
        assert_eq!(PageSize::try_from(Some(10)).map(|s| s.0), Ok(10));
        assert!(PageSize::try_from(Some(0)).is_err());
        assert!(PageSize::try_from(Some(11)).is_err());
    }

    #[test]
    fn cursor_is_bound_to_its_anchor() {
        let (graph, node) = (Uuid::new_v4(), Uuid::new_v4());
        let digest = cursor_digest(graph, node);
        let raw = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&DisagreementCursor {
                revision: CURSOR_REVISION,
                digest: digest.clone(),
                recorded_at: Utc::now(),
                other: Uuid::new_v4(),
            })
            .unwrap(),
        );
        assert!(parse_cursor(Some(&raw), &digest).unwrap().is_some());
        assert!(parse_cursor(Some(&raw), &cursor_digest(graph, Uuid::new_v4())).is_err());
        assert!(parse_cursor(Some("not-a-cursor"), &digest).is_err());
        assert!(parse_cursor(Some(&"a".repeat(2049)), &digest).is_err());
    }

    #[test]
    fn side_refuses_fields_outside_the_contract() {
        let valid = json!({"node_id":Uuid::nil(),"type":"decision","title":null,"evidence_truncated":false,
            "evidence":[{"outcome":{"statement":"s","_id":"x","_at":null},"knowledge_time":null,"effective_time":[],"effective_time_truncated":false}]});
        let parsed = side(valid.clone()).unwrap();
        assert_eq!(serde_json::to_value(&parsed.evidence[0].outcome).unwrap(), json!({"statement":"s"}));
        let mut leaked = valid;
        leaked["raw_title"] = json!("Secret");
        assert!(side(leaked).is_err());
    }
}
