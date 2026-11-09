//! Library topic-index queries.
//!
//! Free functions on a transaction, not `Store` trait methods — Library is
//! a cloud Postgres surface (cross-ocean, numbered pages, `entity_type`
//! column). Same posture as
//! [`super::roster_queries`].

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::store::StoreError;

pub const LIBRARY_PAGE_SIZE: i64 = 20;
pub const LIBRARY_PAGE_SIZE_MAX: i64 = 100;

/// `(offset, page_size)` for a Library page, or `None` when `(page - 1) *
/// page_size` does not fit in `i64`.
pub fn library_page_bounds(page: i64, page_size: i64) -> Option<(i64, i64)> {
    let page = page.max(1);
    let page_size = page_size.clamp(1, LIBRARY_PAGE_SIZE_MAX);
    let offset = page.checked_sub(1)?.checked_mul(page_size)?;
    Some((offset, page_size))
}

const LIBRARY_KINDS: [&str; 4] = ["person", "org", "topic", "project"];

/// Collapse an extractor type string onto Library's four kinds.
/// Unknown / decision / thread / reference all become `topic`.
pub fn library_kind(raw: &str) -> &'static str {
    match raw.trim().to_ascii_lowercase().as_str() {
        "person" | "people" | "human" => "person",
        "org" | "organization" | "organisation" | "company" => "org",
        "project" => "project",
        _ => "topic",
    }
}

pub fn parse_library_kind(raw: &str) -> Option<&'static str> {
    let k = raw.trim().to_ascii_lowercase();
    LIBRARY_KINDS.iter().copied().find(|x| *x == k)
}

pub const LIBRARY_Q_MAX: usize = 200;

fn escape_like(q: &str) -> String {
    let mut out = String::with_capacity(q.len());
    for c in q.chars() {
        match c {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// Title-only `ILIKE` pattern. None means "no title filter".
/// `%` / `_` are literals, not wildcards.
pub fn library_title_pattern(q: &str) -> Option<String> {
    let q = q.trim();
    if q.is_empty() {
        return None;
    }
    Some(format!("%{}%", escape_like(q)))
}

#[derive(Debug, Clone)]
pub struct LibraryTopic {
    pub node_id: Uuid,
    pub title: String,
    pub entity_type: String,
    pub graph_id: Uuid,
    pub graph_name: String,
    pub atom_count: i64,
    pub semantic_atom_count: i64,
    pub source_support_count: i64,
    pub unmapped_legacy_chunk_count: i64,
    pub last_happened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl LibraryTopic {
    pub fn to_json(&self) -> Value {
        json!({
            "node_id": self.node_id,
            "title": self.title,
            "entity_type": self.entity_type,
            "graph_id": self.graph_id,
            "graph_name": self.graph_name,
            "atom_count": self.atom_count,
            "semantic_atom_count": self.semantic_atom_count,
            "source_support_count": self.source_support_count,
            "unmapped_legacy_chunk_count": self.unmapped_legacy_chunk_count,
            "last_happened_at": self.last_happened_at,
            "updated_at": self.updated_at,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LibraryLineage {
    pub node_id: Uuid,
    pub title: String,
    pub graph_id: Uuid,
    pub graph_name: String,
}

#[derive(Debug, Clone)]
pub struct LibraryDecision {
    pub node_id: Uuid,
    pub title: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct LibraryNodeExtras {
    pub entity_type: Option<String>,
    pub entity_type_sticky: bool,
    pub last_happened_at: Option<DateTime<Utc>>,
    pub lineage: Vec<LibraryLineage>,
    pub decisions: Vec<LibraryDecision>,
}

/// `$1` graphs, `$2` entity type, `$3` title ILIKE pattern: how many rows
/// `TOPICS_PAGE_SQL` pages through.
///
/// It does not call `library_node_title`: that ran once per candidate
/// (23,649 x ~3.5 ms on the org tenant, so >120 s and a 500 at the 10 s
/// statement timeout, #1490), almost all of it in `library_atom_permitted`
/// per link. It applies the same visibility predicate inline, with the
/// per-atom permission probe replaced by membership in the graphs' permitted
/// sets (`library_permitted_atoms(g, NULL)`, built once as a hashed subplan;
/// equal per atom, see `super::verdict`). The derivation, against the current
/// definitions (tenant 0137/0157/0161, control 0302/0325/0328):
///
/// `library_node_title(g,n)` is `COALESCE(before(g,n), corrected(g,n))`.
/// Both return `substring(n.title FOR 256)` of the node's own row, `before`
/// when S or L holds and `corrected` when C holds, so the title is non-null
/// exactly when `n.title IS NOT NULL AND (S OR L OR C)`, and when non-null it
/// is `substring(n.title FOR 256)`, which is what `$3` is matched against.
/// - S: a semantic link whose permitted source mentions the title.
/// - L: a legacy link to a permitted atom of the graph whose text contains
///   the title (non-empty), or whose participants include it.
/// - C: an active identity correction labelled with the title whose
///   support is valid.
/// S, L and C below are the function bodies with `g := n.graph_id`,
/// `n_id := n.node_id`, and `library_atom_permitted(g, a.atom_id)` written
/// as `a.atom_id IN (permitted)`. Atom ids are unique and the set for graph
/// `g` holds only atoms of `g`, while L already requires
/// `a.graph_id = n.graph_id`, so one set across `$1` decides the same.
/// The `IS TRUE` keeps that `IN` a hashed subplan: a bare `IN` is pulled up
/// into a join inside the correlated `EXISTS`, which may rescan the set per
/// link. `atom_id IN (..)` is never NULL here (both sides are primary-key
/// ids), so `IS TRUE` does not change the result. Postgres hashes an
/// uncorrelated `IN` only when it estimates the set fits in `hash_mem`, and
/// it estimates a set-returning function at 1000 rows per call (100 calls
/// for a generic `unnest($1)`); the `DISTINCT`, a no-op under `IN`, brings
/// the estimate down to a default group count so the hashed form is always
/// chosen. Each `EXISTS` still stops at the node's first supporting link,
/// as the function did.
///
/// Relations are schema-qualified because the function bodies resolve them
/// with `search_path=pg_catalog,public,pg_temp`; an unqualified name here
/// would let a session's temp table shadow them (`reef_identity_pg`).
/// In `p7_read_boundary_pg`, `library_permission_batching_matches_per_row_sql`
/// compares this with the per-node title count on mixed permitted and
/// forbidden support, and `library_topics_shape_pins` pins the function
/// bodies it was derived from and the hashed probe.
pub const TOPICS_COUNT_SQL: &str = r#"WITH permitted AS MATERIALIZED (
 SELECT p.atom_id FROM unnest($1::uuid[]) g(graph_id)
 CROSS JOIN LATERAL public.library_permitted_atoms(g.graph_id, NULL::uuid[]) p
)
SELECT COUNT(*)::bigint
FROM public.nodes n
WHERE n.graph_id = ANY($1)
  AND n.entity_type IS NOT NULL AND n.title IS NOT NULL
  AND (n.coral_count > 0 OR EXISTS (SELECT 1 FROM public.node_semantic_atoms sa WHERE sa.node_id=n.node_id AND sa.graph_id=n.graph_id))
  AND ($2::text IS NULL OR n.entity_type = $2)
  AND ($3::text IS NULL OR substring(n.title FOR 256) ILIKE $3 ESCAPE '\')
  AND (
   EXISTS(SELECT 1 FROM public.node_semantic_atoms l
    JOIN public.semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
    JOIN public.source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
    JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
    CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e
    WHERE l.graph_id=n.graph_id AND l.node_id=n.node_id AND s.access_state='permitted'
     AND lower(e->>'mention')=lower(n.title))
   OR EXISTS(SELECT 1 FROM public.node_atoms l JOIN public.atoms a ON a.atom_id=l.atom_id AND a.graph_id=n.graph_id
    WHERE l.node_id=n.node_id AND (a.atom_id IN (SELECT DISTINCT pa.atom_id FROM permitted pa)) IS TRUE
     AND n.title<>'' AND (position(lower(n.title) IN lower(COALESCE(a.content_inline,'')))>0
      OR EXISTS(SELECT 1 FROM unnest(a.participants) p WHERE lower(p)=lower(n.title))))
   OR EXISTS(SELECT 1 FROM public.reef_identity_corrections c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id
    AND c.state='active' AND c.label=n.title AND public.reef_identity_support_valid(c))
  )"#;

/// `TOPICS_COUNT_SQL`'s rows, newest first: `$4` limit, `$5` offset.
///
/// The candidates are ordered first and fenced with `OFFSET 0`, and the
/// title runs in a lateral over that ordered stream, so it is computed only
/// until `$5 + $4` titled rows are found rather than for every candidate
/// before the sort (#1490: 83 s to 231 ms on the org tenant, same rows).
/// The order is total (`node_id` breaks ties), so the page is the same rows.
/// The `$3` prefilter on `substring(n.title FOR 256)` is the value the title
/// takes when visible (see `TOPICS_COUNT_SQL`); the title is matched again.
/// The atom counts then admit the page's links with one
/// `library_permitted_atoms` call per graph (see `super::verdict`).
pub const TOPICS_PAGE_SQL: &str = r#"WITH page AS MATERIALIZED (
 SELECT c.node_id, t.title, c.entity_type, c.graph_id, c.name, c.updated_at
 FROM (SELECT n.node_id, n.graph_id, n.entity_type, g.name, n.updated_at
       FROM nodes n
       JOIN graphs g ON g.id = n.graph_id
       WHERE n.graph_id = ANY($1)
         AND n.entity_type IS NOT NULL AND n.title IS NOT NULL
         AND (n.coral_count > 0 OR EXISTS (SELECT 1 FROM node_semantic_atoms sa WHERE sa.node_id=n.node_id AND sa.graph_id=n.graph_id))
         AND ($2::text IS NULL OR n.entity_type = $2)
         AND ($3::text IS NULL OR substring(n.title FOR 256) ILIKE $3 ESCAPE '\')
       ORDER BY n.updated_at DESC, n.node_id DESC
       OFFSET 0) c
 CROSS JOIN LATERAL (SELECT library_node_title(c.graph_id,c.node_id) AS title OFFSET 0) t
 WHERE t.title IS NOT NULL AND ($3::text IS NULL OR t.title ILIKE $3 ESCAPE '\')
 ORDER BY c.updated_at DESC, c.node_id DESC
 LIMIT $4 OFFSET $5
), links AS MATERIALIZED (
 SELECT p.graph_id, l.node_id, l.atom_id FROM page p JOIN node_atoms l ON l.node_id = p.node_id
), permitted AS MATERIALIZED (
 SELECT i.graph_id, x.atom_id
 FROM (SELECT l.graph_id, array_agg(DISTINCT l.atom_id) AS ids FROM links l GROUP BY l.graph_id) i
 CROSS JOIN LATERAL library_permitted_atoms(i.graph_id, CASE WHEN cardinality(i.ids) > 10000 THEN NULL ELSE i.ids END) x
), counts AS MATERIALIZED (
 SELECT l.node_id, count(*) AS n FROM links l JOIN permitted p ON p.graph_id = l.graph_id AND p.atom_id = l.atom_id GROUP BY l.node_id
)
SELECT p.node_id, p.title, p.entity_type, p.graph_id, p.name, COALESCE(c.n, 0), NULL::timestamptz, p.updated_at
FROM page p LEFT JOIN counts c ON c.node_id = p.node_id
ORDER BY p.updated_at DESC, p.node_id DESC"#;

/// `$1` node, `$2` reachable graphs, `$3` the node it was promoted from.
pub(crate) const LINEAGE_SQL: &str = r#"SELECT n.node_id, t.title, n.graph_id, g.name
FROM nodes n
JOIN graphs g ON g.id = n.graph_id
CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
WHERE n.node_id <> $1
  AND n.graph_id = ANY($2) AND t.title IS NOT NULL
  AND (
        n.promoted_from_node_id = $1
     OR ($3::uuid IS NOT NULL AND n.node_id = $3)
     OR ($3::uuid IS NOT NULL AND n.promoted_from_node_id = $3)
  )
ORDER BY g.name ASC, t.title ASC"#;

/// `$1` node: the decisions sharing a readable atom with it, newest first.
pub(crate) const DECISIONS_SQL: &str = r#"SELECT d.node_id, t.title, d.created_at
FROM nodes d
CROSS JOIN LATERAL (SELECT library_node_title(d.graph_id,d.node_id) AS title OFFSET 0) t
WHERE d.type = 'decision' AND t.title IS NOT NULL
  AND d.graph_id = (SELECT graph_id FROM nodes WHERE node_id = $1)
  AND EXISTS (
        SELECT 1
          FROM node_atoms da
          JOIN node_atoms ta ON da.atom_id = ta.atom_id
         WHERE da.node_id = d.node_id
           AND ta.node_id = $1 AND library_atom_permitted(d.graph_id,da.atom_id)
  )
ORDER BY d.created_at DESC
LIMIT 20"#;

/// Typed topics with at least one retrieval coral or semantic atom, in `graph_ids`, newest `updated_at` first.
///
/// `offset` / `limit` are already validated by the caller
/// ([`library_page_bounds`], or a per-account window of `offset + page_size`).
pub async fn list_library_topics(
    tx: &mut Transaction<'_, Postgres>,
    graph_ids: &[Uuid],
    entity_type: Option<&str>,
    title_q: Option<&str>,
    offset: i64,
    limit: i64,
) -> Result<(Vec<LibraryTopic>, i64), StoreError> {
    if graph_ids.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let offset = offset.max(0);
    let limit = limit.max(1);
    let like = title_q.and_then(library_title_pattern);

    let total: i64 = sqlx::query_scalar(TOPICS_COUNT_SQL)
    .bind(graph_ids)
    .bind(entity_type)
    .bind(like.as_deref())
    .fetch_one(&mut **tx)
    .await?;

    type Row = (
        Uuid,
        String,
        String,
        Uuid,
        String,
        i64,
        Option<DateTime<Utc>>,
        DateTime<Utc>,
    );
    let rows: Vec<Row> = sqlx::query_as(TOPICS_PAGE_SQL)
    .bind(graph_ids)
    .bind(entity_type)
    .bind(like.as_deref())
    .bind(limit)
    .bind(offset)
    .fetch_all(&mut **tx)
    .await?;

    let mut topics: Vec<LibraryTopic> = rows
        .into_iter()
        .map(
            |(
                node_id,
                title,
                entity_type,
                graph_id,
                graph_name,
                atom_count,
                last_happened_at,
                updated_at,
            )| LibraryTopic {
                node_id,
                title,
                entity_type,
                graph_id,
                graph_name,
                atom_count,
                semantic_atom_count: 0,
                source_support_count: 0,
                unmapped_legacy_chunk_count: 0,
                last_happened_at,
                updated_at,
            },
        )
        .collect();
    let node_ids: Vec<Uuid> = topics.iter().map(|topic| topic.node_id).collect();
    let counts = crate::source_evidence::node_evidence_counts_for_graphs_tx(tx, graph_ids, &node_ids).await?;
    let counts: std::collections::HashMap<_, _> = counts.into_iter().map(|count| (count.node_id, count)).collect();
    for topic in &mut topics {
        if let Some(count) = counts.get(&topic.node_id) {
            topic.semantic_atom_count = count.semantic_atom_count;
            topic.source_support_count = count.source_support_count;
            topic.unmapped_legacy_chunk_count = count.unmapped_legacy_chunk_count;
        }
    }
    Ok((topics, total))
}

/// Persist a kind from extract / 100-coral re-eval. No-op when sticky.
pub async fn apply_library_kind(
    tx: &mut Transaction<'_, Postgres>,
    node_id: Uuid,
    entity_type: &str,
) -> Result<(), StoreError> {
    let kind = library_kind(entity_type);
    sqlx::query(
        "UPDATE nodes \
            SET body = jsonb_set(coalesce(body, '{}'::jsonb), '{entity_type}', to_jsonb($2::text)), \
                entity_type = $2, \
                kind_eval_coral_count = coral_count \
          WHERE node_id = $1 \
            AND entity_type_sticky = false",
    )
    .bind(node_id)
    .bind(kind)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Human-set kind. Always sticky until a later human clear (not exposed yet).
pub async fn set_library_kind_sticky(
    tx: &mut Transaction<'_, Postgres>,
    node_id: Uuid,
    entity_type: &str,
) -> Result<(), StoreError> {
    let Some(kind) = parse_library_kind(entity_type) else {
        return Err(StoreError::Db(Box::from(
            "entity_type must be person, org, topic, or project",
        )));
    };
    let n = sqlx::query(
        "UPDATE nodes \
            SET body = jsonb_set(coalesce(body, '{}'::jsonb), '{entity_type}', to_jsonb($2::text)), \
                entity_type = $2, \
                entity_type_sticky = true \
          WHERE node_id = $1",
    )
    .bind(node_id)
    .bind(kind)
    .execute(&mut **tx)
    .await?;
    if n.rows_affected() == 0 {
        return Err(StoreError::NotFound);
    }
    Ok(())
}

pub async fn library_node_extras(
    tx: &mut Transaction<'_, Postgres>,
    node_id: Uuid,
    reachable_graph_ids: &[Uuid],
) -> Result<LibraryNodeExtras, StoreError> {
    let row: Option<(Option<String>, bool, Option<DateTime<Utc>>, Option<Uuid>)> = sqlx::query_as(
        "SELECT entity_type, entity_type_sticky, NULL::timestamptz, promoted_from_node_id \
         FROM nodes WHERE node_id = $1",
    )
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((entity_type, entity_type_sticky, last_happened_at, promoted_from)) = row else {
        return Ok(LibraryNodeExtras {
            entity_type: None,
            entity_type_sticky: false,
            last_happened_at: None,
            lineage: Vec::new(),
            decisions: Vec::new(),
        });
    };

    type LinRow = (Uuid, String, Uuid, String);
    let lineage_rows: Vec<LinRow> = sqlx::query_as(LINEAGE_SQL)
    .bind(node_id)
    .bind(reachable_graph_ids)
    .bind(promoted_from)
    .fetch_all(&mut **tx)
    .await?;
    let lineage = lineage_rows
        .into_iter()
        .map(|(node_id, title, graph_id, graph_name)| LibraryLineage {
            node_id,
            title,
            graph_id,
            graph_name,
        })
        .collect();

    type DecRow = (Uuid, String, DateTime<Utc>);
    let decision_rows: Vec<DecRow> = sqlx::query_as(DECISIONS_SQL)
    .bind(node_id)
    .fetch_all(&mut **tx)
    .await?;
    let decisions = decision_rows
        .into_iter()
        .map(|(node_id, title, created_at)| LibraryDecision {
            node_id,
            title,
            created_at,
        })
        .collect();

    Ok(LibraryNodeExtras {
        entity_type,
        entity_type_sticky,
        last_happened_at,
        lineage,
        decisions,
    })
}

/// Current Library kind column, if the node exists.
pub async fn persisted_library_kind(
    tx: &mut Transaction<'_, Postgres>,
    node_id: Uuid,
) -> Result<Option<String>, StoreError> {
    let kind: Option<Option<String>> =
        sqlx::query_scalar::<_, Option<String>>("SELECT entity_type FROM nodes WHERE node_id = $1")
            .bind(node_id)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(kind.flatten())
}

pub async fn kind_reeval_context(
    tx: &mut Transaction<'_, Postgres>,
    node_id: Uuid,
) -> Result<Option<(String, bool, i64, Vec<String>)>, StoreError> {
    let row: Option<(String, bool, i64)> = sqlx::query_as(
        "SELECT title, entity_type_sticky, coral_count::bigint \
         FROM nodes WHERE node_id = $1",
    )
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((title, sticky, coral_count)) = row else {
        return Ok(None);
    };
    let atom_titles: Vec<String> = sqlx::query_scalar(
        "SELECT COALESCE(NULLIF(a.title, ''), '') \
         FROM node_atoms na \
         JOIN atoms a ON a.atom_id = na.atom_id \
         WHERE na.node_id = $1 \
         ORDER BY a.timestamp DESC NULLS LAST \
         LIMIT 24",
    )
    .bind(node_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(Some((title, sticky, coral_count, atom_titles)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn library_kind_maps_extractor_types() {
        assert_eq!(library_kind("person"), "person");
        assert_eq!(library_kind("ORG"), "org");
        assert_eq!(library_kind("company"), "org");
        assert_eq!(library_kind("project"), "project");
        assert_eq!(library_kind("decision"), "topic");
        assert_eq!(library_kind("thread"), "topic");
        assert_eq!(library_kind(""), "topic");
    }

    #[test]
    fn parse_library_kind_rejects_unknown() {
        assert_eq!(parse_library_kind("person"), Some("person"));
        assert_eq!(parse_library_kind("PERSON"), Some("person"));
        assert_eq!(parse_library_kind("person."), None);
        assert_eq!(parse_library_kind("decision"), None);
        assert_eq!(parse_library_kind(""), None);
    }

    #[test]
    fn library_page_bounds_rejects_overflow() {
        assert_eq!(library_page_bounds(1, 20), Some((0, 20)));
        assert_eq!(library_page_bounds(2, 20), Some((20, 20)));
        assert_eq!(library_page_bounds(0, 20), Some((0, 20)));
        assert_eq!(library_page_bounds(1, 0), Some((0, 1)));
        assert_eq!(library_page_bounds(i64::MAX, 20), None);
        assert_eq!(
            library_page_bounds(i64::MAX / 2, LIBRARY_PAGE_SIZE_MAX),
            None
        );
    }

    #[test]
    fn escape_like_escapes_wildcards() {
        assert_eq!(escape_like("100%_\\x"), "100\\%\\_\\\\x");
    }

    #[test]
    fn library_title_pattern_is_substring_and_literal() {
        assert_eq!(library_title_pattern("  "), None);
        assert_eq!(library_title_pattern("Clerk"), Some("%Clerk%".into()));
        assert_eq!(library_title_pattern("  Face  "), Some("%Face%".into()));
        assert_eq!(library_title_pattern("%"), Some("%\\%%".into()));
        assert_eq!(library_title_pattern("_"), Some("%\\_%".into()));
    }
}
