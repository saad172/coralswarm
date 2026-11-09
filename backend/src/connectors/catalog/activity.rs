//! Connector activity shares Library admission, including history floors and retractions.
use super::{summary::TopicSummary, AdapterCx, AdapterError, ConnectorAdapter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureQuery {
    pub source: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct Capture {
    pub atom_id: Uuid,
    pub source: String,
    pub title: Option<String>,
    pub excerpt: Option<String>,
    pub author: Option<String>,
    #[schema(value_type = String, format = DateTime)]
    pub captured_at: DateTime<Utc>,
    pub meta: Value,
    pub topics: Vec<TopicSummary>,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct CapturePage {
    pub items: Vec<Capture>,
    pub next_cursor: Option<String>,
}
fn invalid() -> AdapterError {
    AdapterError::Refused {
        status: 400,
        code: "invalid_capture_query",
    }
}
fn types(
    adapter: &dyn ConnectorAdapter,
    source: Option<&str>,
) -> Result<Vec<String>, AdapterError> {
    if source.is_some_and(|key| !adapter.sources().iter().any(|s| s.key == key)) {
        return Err(invalid());
    }
    Ok(adapter
        .sources()
        .iter()
        .filter(|s| source.is_none_or(|key| s.key == key))
        .flat_map(|s| s.atom_source_types.iter().map(|t| t.to_string()))
        .collect())
}
// Text is projected only from the Library-admitted relation. No content blobs,
// credentials or arbitrary provider metadata are returned.
const ADMITTED: &str = crate::library_read::CONNECTOR_ACTIVITY_CTE;

/// Capture activity of the caller's own atoms, admitted once per worker compute and
/// kept per atom `source_type`; [`ActivityByType::stats`] folds it per
/// source spec. `GET /v1/connectors` used to run the admitted relation once
/// per source spec (about twelve whole-ocean admissions per request, 1.7 s
/// each on a 600k-atom seed, 10 s per request on production). One statement
/// over the caller's atoms costs 1.0 s on the same seed.
#[derive(Debug, Default)]
pub struct ActivityByType {
    rows: Vec<TypeActivity>,
}

#[derive(Debug)]
struct TypeActivity {
    source_type: String,
    last: Option<DateTime<Utc>>,
    week: [u32; 7],
}

/// `$1` account, `$2` user, `$3` readable graphs. Candidates are the caller's
/// own atoms (`idx_atoms_connector_activity`), admitted by the Library's rule
/// per ocean; above the ceiling the whole ocean is admitted once instead of
/// probing per id, as `services::home_metrics` and `store::library` do.
/// `week` is oldest day first, UTC calendar days, the same shape the per-spec
/// statement produced.
/// Materialize permission results before joining candidates: PostgreSQL 16
/// otherwise can put the lateral function inside the candidate join and admit
/// the entire graph again for every atom. This fence is not an admission cap.
const ACTIVITY_BY_TYPE_SQL: &str = r#"
WITH cand AS MATERIALIZED (
  SELECT a.graph_id, a.atom_id, a.source_type, a.created_at
  FROM graphs g JOIN atoms a ON a.graph_id = g.id
  WHERE g.account_id = $1 AND g.id = ANY($3::uuid[]) AND a.ingested_by = $2
), per_graph AS (
  SELECT graph_id, array_agg(atom_id) AS ids FROM cand GROUP BY graph_id
), permitted AS MATERIALIZED (
  SELECT pg.graph_id, p.atom_id
  FROM per_graph pg
  CROSS JOIN LATERAL library_permitted_atoms(pg.graph_id,
    CASE WHEN cardinality(pg.ids) > 10000 THEN NULL ELSE pg.ids END) p
), admitted AS MATERIALIZED (
  SELECT c.source_type, c.created_at, (c.created_at AT TIME ZONE 'UTC')::date AS day
  FROM permitted p
  JOIN cand c ON c.graph_id = p.graph_id AND c.atom_id = p.atom_id
), today AS (SELECT (now() AT TIME ZONE 'UTC')::date AS d)
SELECT a.source_type, max(a.created_at) AS last,
       ARRAY[count(*) FILTER (WHERE a.day = t.d - 6), count(*) FILTER (WHERE a.day = t.d - 5),
             count(*) FILTER (WHERE a.day = t.d - 4), count(*) FILTER (WHERE a.day = t.d - 3),
             count(*) FILTER (WHERE a.day = t.d - 2), count(*) FILTER (WHERE a.day = t.d - 1),
             count(*) FILTER (WHERE a.day = t.d)] AS week
FROM admitted a CROSS JOIN today t GROUP BY a.source_type
"#;

impl ActivityByType {
    /// Existing mutation responses report live action results under Other. Page
    /// GETs never call this opener; workers use load_scoped directly.
    pub async fn load_action(cx: &AdapterCx<'_>) -> Result<Self, AdapterError> {
        if cx.tenant.pool.is_none() {
            return Ok(Self::default());
        }
        let graphs = crate::library_read::connector_activity_graphs(cx.tenant, cx.user).await?;
        let mut tx = cx.tenant.begin_member_scoped(cx.account_id).await?;
        Self::load_scoped(&mut tx, cx.account_id, cx.user_id, &graphs).await
    }
    /// Empty when the request has no content plane: counts are zero and
    /// nothing was captured, as before.
    pub async fn load_scoped(
        tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
        account_id: Uuid,
        user_id: Uuid,
        graphs: &[Uuid],
    ) -> Result<Self, AdapterError> {
        let rows = sqlx::query(ACTIVITY_BY_TYPE_SQL)
            .bind(account_id)
            .bind(user_id)
            .bind(graphs)
            .fetch_all(&mut **tx)
            .await?;
        let rows = rows
            .iter()
            .map(|r| {
                let counts: Vec<i64> = r.get("week");
                TypeActivity {
                    source_type: r.get("source_type"),
                    last: r.get("last"),
                    week: std::array::from_fn(|i| counts[i].min(u32::MAX as i64) as u32),
                }
            })
            .collect();
        Ok(Self { rows })
    }

    /// Newest capture and the last seven days of counts for a spec's atom
    /// source types, with the same type rule as [`ADMITTED`].
    pub fn stats(&self, types: &[&str]) -> (Option<DateTime<Utc>>, [u32; 7]) {
        let mut last = None;
        let mut week = [0u32; 7];
        for row in self
            .rows
            .iter()
            .filter(|r| type_matches(types, &r.source_type))
        {
            last = last.max(row.last);
            for (w, n) in week.iter_mut().zip(row.week) {
                *w = w.saturating_add(n);
            }
        }
        (last, week)
    }
}

/// The admitted relation's type rule: an exact `source_type`, or, for the
/// toolkits whose atoms carry a `family_` prefix (`jira_issue`, `linear_...`),
/// any type of that family.
fn type_matches(types: &[&str], source_type: &str) -> bool {
    const FAMILIES: [&str; 6] = ["jira", "linear", "confluence", "hubspot", "zendesk", "dart"];
    types.iter().any(|t| {
        if *t == source_type {
            return true;
        }
        let family = t.split('_').next().unwrap_or("");
        FAMILIES.contains(&family) && source_type.starts_with(&format!("{family}_"))
    })
}

pub async fn topics(
    cx: &AdapterCx<'_>,
    adapter: &dyn ConnectorAdapter,
) -> Result<Vec<TopicSummary>, AdapterError> {
    let graphs = crate::library_read::connector_activity_graphs(cx.tenant, cx.user).await?;
    let mut tx = cx.tenant.begin_member_scoped(cx.account_id).await?;
    topics_scoped(&mut tx, cx.account_id, cx.user_id, &graphs, adapter).await
}

pub async fn captures(
    cx: &AdapterCx<'_>,
    adapter: &dyn ConnectorAdapter,
    query: CaptureQuery,
) -> Result<CapturePage, AdapterError> {
    let graphs = crate::library_read::connector_activity_graphs(cx.tenant, cx.user).await?;
    let mut tx = cx.tenant.begin_member_scoped(cx.account_id).await?;
    captures_scoped(&mut tx, cx.account_id, cx.user_id, &graphs, adapter, query).await
}

pub async fn topics_scoped(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    user_id: Uuid,
    graphs: &[Uuid],
    adapter: &dyn ConnectorAdapter,
) -> Result<Vec<TopicSummary>, AdapterError> {
    let sql = format!("{ADMITTED} SELECT n.node_id,n.graph_id,count(DISTINCT a.atom_id)::bigint total,count(DISTINCT a.atom_id) FILTER (WHERE a.created_at>=now()-interval '7 days')::bigint week FROM admitted a JOIN node_atoms na ON na.atom_id=a.atom_id JOIN nodes n ON n.node_id=na.node_id AND n.graph_id=a.graph_id GROUP BY n.node_id,n.title,n.graph_id ORDER BY total DESC,n.node_id");
    let rows = sqlx::query(&sql)
        .bind(account_id)
        .bind(user_id)
        .bind(types(adapter, None)?)
        .bind(graphs)
        .fetch_all(&mut **tx)
        .await?;
    // This is a fast batch, never an admission cap. Most pages find twelve
    // titles here; otherwise project the entire remainder once. At most two
    // bulk projections, no per-row SQL, and no eligible title is skipped.
    let (first, rest) = rows.split_at(rows.len().min(64));
    let mut topics = project_topics(tx, first, 12).await?;
    if topics.len() < 12 && !rest.is_empty() {
        topics.extend(project_topics(tx, rest, 12 - topics.len()).await?);
    }
    Ok(topics)
}

async fn project_topics(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    rows: &[sqlx::postgres::PgRow],
    limit: usize,
) -> Result<Vec<TopicSummary>, AdapterError> {
    let gs: Vec<Uuid> = rows.iter().map(|r| r.get("graph_id")).collect();
    let ns: Vec<Uuid> = rows.iter().map(|r| r.get("node_id")).collect();
    let titles = crate::services::journal_activities::project_node_titles(tx, &gs, &ns).await?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            let id = r.get("node_id");
            titles
                .get(&(r.get("graph_id"), id))
                .map(|title| TopicSummary {
                    id,
                    label: title.clone(),
                    total: r.get::<i64, _>("total").min(u32::MAX as i64) as u32,
                    last_7_days: r.get::<i64, _>("week").min(u32::MAX as i64) as u32,
                })
        })
        .take(limit)
        .collect())
}

pub async fn captures_scoped(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    user_id: Uuid,
    graphs: &[Uuid],
    adapter: &dyn ConnectorAdapter,
    query: CaptureQuery,
) -> Result<CapturePage, AdapterError> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    let source_types = types(adapter, query.source.as_deref())?;
    let limit = query.limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(invalid());
    }
    let cursor: Option<(String, Option<String>, DateTime<Utc>, Uuid)> = query
        .cursor
        .as_ref()
        .map(|v| {
            URL_SAFE_NO_PAD
                .decode(v)
                .map_err(|_| invalid())
                .and_then(|b| serde_json::from_slice(&b).map_err(|_| invalid()))
        })
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|c| c.0 != adapter.key() || c.1 != query.source)
    {
        return Err(invalid());
    }
    let sql = format!("{ADMITTED} SELECT atom_id,source_type,title,substring(excerpt FOR 500) excerpt,author_name,created_at,channel_name FROM admitted WHERE ($5::timestamptz IS NULL OR (created_at,atom_id)<($5,$6)) ORDER BY created_at DESC,atom_id DESC LIMIT $7");
    let rows = sqlx::query(&sql)
        .bind(account_id)
        .bind(user_id)
        .bind(source_types)
        .bind(graphs)
        .bind(cursor.as_ref().map(|c| c.2))
        .bind(cursor.as_ref().map(|c| c.3))
        .bind(i64::from(limit) + 1)
        .fetch_all(&mut **tx)
        .await?;
    let more = rows.len() > limit as usize;
    let mut items: Vec<Capture> = rows
        .iter()
        .take(limit as usize)
        .map(|r| {
            let kind: String = r.get("source_type");
            Capture {
                atom_id: r.get("atom_id"),
                source: adapter
                    .sources()
                    .iter()
                    .find(|s| {
                        s.atom_source_types.contains(&kind.as_str())
                            || (adapter.beta() && s.key == "workspace")
                    })
                    .unwrap()
                    .key
                    .to_string(),
                title: r.get("title"),
                excerpt: r.get("excerpt"),
                author: r.get("author_name"),
                captured_at: r.get("created_at"),
                meta: serde_json::json!({"channel":r.get::<Option<String>,_>("channel_name")}),
                topics: Vec::new(),
            }
        })
        .collect();
    // One link query and one admitted title projection for the whole page.
    // Apply each capture's deterministic twelve-topic cap after title admission.
    let ids: Vec<Uuid> = items.iter().map(|i| i.atom_id).collect();
    let links: Vec<(Uuid, Uuid, Uuid)> = sqlx::query_as(
        "/* history_floor: atom keys admitted by the page query in this snapshot */ \
         SELECT na.atom_id,n.graph_id,n.node_id FROM node_atoms na JOIN nodes n ON n.node_id=na.node_id \
         JOIN atoms a ON a.atom_id=na.atom_id AND a.graph_id=n.graph_id \
         WHERE na.atom_id=ANY($1) AND n.graph_id=ANY($2) ORDER BY na.atom_id,n.node_id",
    ).bind(&ids).bind(graphs).fetch_all(&mut **tx).await?;
    let gs: Vec<_> = links.iter().map(|(_, g, _)| *g).collect();
    let ns: Vec<_> = links.iter().map(|(_, _, n)| *n).collect();
    let titles = crate::services::journal_activities::project_node_titles(tx, &gs, &ns).await?;
    for item in &mut items {
        item.topics = links
            .iter()
            .filter(|(a, _, _)| *a == item.atom_id)
            .filter_map(|(_, graph, id)| {
                titles.get(&(*graph, *id)).map(|label| TopicSummary {
                    id: *id,
                    label: label.clone(),
                    total: 1,
                    last_7_days: u32::from(
                        item.captured_at >= Utc::now() - chrono::Duration::days(7),
                    ),
                })
            })
            .take(12)
            .collect();
    }
    let next_cursor = if more {
        items.last().map(|i| {
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&(adapter.key(), &query.source, i.captured_at, i.atom_id))
                    .expect("serializable cursor"),
            )
        })
    } else {
        None
    };
    Ok(CapturePage { items, next_cursor })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(t: &str, week: [u32; 7], last_secs: i64) -> TypeActivity {
        TypeActivity {
            source_type: t.into(),
            last: Some(DateTime::<Utc>::from_timestamp(last_secs, 0).unwrap()),
            week,
        }
    }

    #[test]
    fn folds_exact_and_family_types_like_the_admitted_relation() {
        let a = ActivityByType {
            rows: vec![
                row("slack", [1, 0, 0, 0, 0, 0, 2], 10),
                row("jira_issue", [0, 1, 0, 0, 0, 0, 0], 30),
                row("jira_comment", [0, 0, 1, 0, 0, 0, 0], 20),
                row("teams_chat", [5, 5, 5, 5, 5, 5, 5], 40),
            ],
        };
        assert_eq!(
            a.stats(&["slack"]),
            (
                Some(DateTime::from_timestamp(10, 0).unwrap()),
                [1, 0, 0, 0, 0, 0, 2]
            )
        );
        assert_eq!(
            a.stats(&["jira_issue"]),
            (
                Some(DateTime::from_timestamp(30, 0).unwrap()),
                [0, 1, 1, 0, 0, 0, 0]
            )
        );
        assert_eq!(a.stats(&["gmail"]), (None, [0; 7]));
        assert!(!type_matches(&["teams_chat"], "teams_channel"));
        assert!(!type_matches(&["jira_issue"], "jiraish_thing"));
    }
}
