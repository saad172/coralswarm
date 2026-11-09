//! Per-row verdicts: STABLE SQL functions that read tables to decide what a
//! row may show. Postgres runs every textual call, so a verdict named in a
//! filter and again in the select list runs twice per row, and a verdict
//! column of a subquery is copied into each outer reference when the planner
//! pulls the subquery up (#1486: five `library_answer_permitted` calls per
//! assistant turn). A statement computes each verdict once per row, in
//! `CROSS JOIN LATERAL (SELECT verdict(..) AS v OFFSET 0) alias`, and every
//! other use reads `alias.v`. `OFFSET 0` stops the planner pulling the lateral
//! up and substituting the call back into each reference. The lateral runs per
//! source row after the source's own filters, and rows stream through it, so
//! an `ORDER BY .. LIMIT` served by an index still stops early.
//!
//! Readable-atom counts are not a per-row verdict. `library_atom_permitted`
//! sets `search_path`, so Postgres cannot inline it and each call re-runs
//! `library_permitted_atoms` for one atom (~2 ms on the org tenant, #1490): a
//! count over a node's links cost one call per link, and a 97k-link page took
//! over 120 s. A statement that counts readable links instead collects the
//! links once, admits their atoms with ONE `library_permitted_atoms(g, ids)`
//! call per graph, and joins. `library_atom_permitted(g,a)` is defined as
//! `EXISTS(SELECT 1 FROM library_permitted_atoms(g,ARRAY[a]))`, and every
//! predicate in `library_permitted_atoms` reads only the candidate atom row
//! and `g`, so the set for `ids` is exactly the atoms of `ids` that pass one
//! by one. Its whole-graph branch (`ids` NULL) applies the same predicates to
//! every atom of `g` (its `mapped` aggregate equals the per-atom lateral), so
//! above `PERMITTED_IDS_MAX` distinct atoms the statements pass NULL, which
//! one graph scan answers faster than 10k index probes; the join to the links
//! keeps the count to the links either way. `atoms` is keyed by `atom_id` and
//! `node_atoms` by `(node_id, atom_id)`, so the join counts each permitted
//! link once, as the per-row `count(*)` did. An empty `ARRAY(..)` is `'{}'`,
//! never NULL, so no links never selects the whole-graph branch.
//!
//! `scripts/check_verdict_once.py` reads `VERDICT_FUNCTIONS` from this file
//! and fails on a SQL literal that calls one of them twice with the same
//! arguments. `p7_read_boundary_pg` plans each of `STATEMENTS` and fails when
//! the plan evaluates a verdict more often than the SQL names it.

/// The verdict functions, as `scripts/check_verdict_once.py` parses them.
pub const VERDICT_FUNCTIONS: &[&str] = &[
    "library_answer_permitted",
    "library_answer_permitted_before_temporal",
    "library_conversation_title",
    "library_node_title",
    "library_node_permitted",
    "library_atom_permitted",
    "library_edge_permitted",
    "library_legacy_headline_sha256",
    "library_session_supported",
];

/// `$1` conversation, `$2` page cursor seq, `$3` limit.
pub const CONVERSATION_PAGE_SQL: &str = r#"SELECT m.id, m.conversation_id, m.seq, m.role, CASE WHEN v.support_verified THEN m.text ELSE '[Evidence unavailable: retained support cannot be verified]' END AS text, CASE WHEN v.support_verified THEN m.citations ELSE '[]'::jsonb END AS citations, CASE WHEN v.support_verified THEN m.sources ELSE '[]'::jsonb END AS sources,
       m.mode, CASE WHEN v.support_verified THEN m.timing ELSE jsonb_build_object('support_status','unavailable') END AS timing, CASE WHEN v.support_verified THEN m.scope_graph_ids ELSE '[]'::jsonb END AS scope_graph_ids, m.created_at, v.support_verified
FROM (SELECT * FROM conversation_messages WHERE conversation_id=$1 AND ($2::int IS NULL OR seq < $2) ORDER BY seq DESC LIMIT $3) m
CROSS JOIN LATERAL (SELECT m.role='user' OR library_answer_permitted(m.id) AS support_verified OFFSET 0) v
ORDER BY m.seq ASC"#;

/// `$1` account, `$2` caller, `$3` limit, `$4` offset.
pub const QUERY_HISTORY_PAGE_SQL: &str = r#"SELECT q.id, q.graph_id, q.user_id, q.query_text, CASE WHEN v.permitted THEN q.answer_summary ELSE ''::text END AS answer_summary,
       CASE WHEN v.permitted THEN q.citation_count ELSE 0 END AS citation_count, q.input_tokens, q.output_tokens, q.created_at
FROM (SELECT * FROM query_history WHERE account_id = $1 AND (user_id = $2 OR user_id IS NULL)
      ORDER BY created_at DESC LIMIT $3 OFFSET $4) q
CROSS JOIN LATERAL (SELECT library_answer_permitted(q.id) AS permitted OFFSET 0) v
ORDER BY q.created_at DESC"#;

/// `$1` account, `$2` caller, `$3` graph, `$4` limit, `$5` offset.
pub const QUERY_HISTORY_GRAPH_PAGE_SQL: &str = r#"SELECT q.id, q.graph_id, q.user_id, q.query_text, CASE WHEN v.permitted THEN q.answer_summary ELSE ''::text END AS answer_summary,
       CASE WHEN v.permitted THEN q.citation_count ELSE 0 END AS citation_count, q.input_tokens, q.output_tokens, q.created_at
FROM (SELECT * FROM query_history WHERE account_id = $1 AND (user_id = $2 OR user_id IS NULL) AND graph_id = $3
      ORDER BY created_at DESC LIMIT $4 OFFSET $5) q
CROSS JOIN LATERAL (SELECT library_answer_permitted(q.id) AS permitted OFFSET 0) v
ORDER BY q.created_at DESC"#;

/// `$1` graph, `$2` owner, `$3` ILIKE pattern, `$4` limit.
pub const CONVERSATION_SEARCH_SQL: &str = r#"SELECT DISTINCT c.id, c.account_id, c.graph_id, c.user_id, t.title,
       c.pinned, c.shared, c.mode, c.created_at, c.updated_at
FROM conversations c
CROSS JOIN LATERAL (SELECT library_conversation_title(c.id) AS title OFFSET 0) t
LEFT JOIN conversation_messages m ON m.conversation_id = c.id
WHERE c.graph_id = $1 AND c.user_id = $2
  AND (t.title ILIKE $3 OR ((m.role='user' OR library_answer_permitted(m.id)) AND m.text ILIKE $3))
ORDER BY c.updated_at DESC
LIMIT $4"#;

/// `$1` graph, `$2` node ids: the readable ones with their titles.
pub const NODE_TITLES_SQL: &str = r#"SELECT n.node_id, t.title FROM nodes n
CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
WHERE n.graph_id = $1 AND n.node_id = ANY($2) AND t.title IS NOT NULL"#;

/// Distinct link atoms above which a permitted-atom count asks
/// `library_permitted_atoms` for the whole graph instead of the id list
/// (measured crossover on the org tenant, #1490). Spelled as the literal
/// `10000` in the SQL below; `permitted_ids_max_matches_sql` pins the two.
pub const PERMITTED_IDS_MAX: usize = 10_000;

/// `$1` graph, `$2` node ids: `NODE_TITLES_SQL` plus each node's readable atom
/// count, from one permitted-atom call for all the titled nodes' links.
pub const NODE_TITLES_WITH_COUNTS_SQL: &str = r#"WITH titled AS MATERIALIZED (
 SELECT n.node_id, t.title FROM nodes n
 CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
 WHERE n.graph_id = $1 AND n.node_id = ANY($2) AND t.title IS NOT NULL
), links AS MATERIALIZED (
 SELECT l.node_id, l.atom_id FROM node_atoms l WHERE l.node_id = ANY(ARRAY(SELECT node_id FROM titled))
), permitted AS MATERIALIZED (
 SELECT p.atom_id FROM (SELECT ARRAY(SELECT DISTINCT atom_id FROM links) AS ids) i
 CROSS JOIN LATERAL library_permitted_atoms($1, CASE WHEN cardinality(i.ids) > 10000 THEN NULL ELSE i.ids END) p
), counts AS MATERIALIZED (
 SELECT l.node_id, count(*) AS n FROM links l JOIN permitted p ON p.atom_id = l.atom_id GROUP BY l.node_id
)
SELECT t.node_id, t.title, COALESCE(c.n, 0) FROM titled t LEFT JOIN counts c ON c.node_id = t.node_id"#;

/// `$1` node: its title and no cached digest, when readable.
pub const NODE_DIGEST_SQL: &str = r#"SELECT t.title, NULL::text FROM nodes n
CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
WHERE n.node_id = $1 AND t.title IS NOT NULL"#;

/// `$1` node: the digest worker's context, when readable. The readable atom
/// count comes from one permitted-atom call over the node's links.
pub const NODE_DIGEST_CONTEXT_SQL: &str = r#"WITH reef AS MATERIALIZED (
 SELECT n.node_id, n.graph_id, n.type, t.title FROM nodes n
 CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t
 WHERE n.node_id = $1 AND t.title IS NOT NULL
), links AS MATERIALIZED (
 SELECT l.atom_id FROM reef JOIN node_atoms l ON l.node_id = reef.node_id
), permitted AS MATERIALIZED (
 SELECT p.atom_id FROM reef
 CROSS JOIN LATERAL (SELECT ARRAY(SELECT atom_id FROM links) AS ids) i
 CROSS JOIN LATERAL library_permitted_atoms(reef.graph_id, CASE WHEN cardinality(i.ids) > 10000 THEN NULL ELSE i.ids END) p
)
SELECT reef.graph_id, reef.type, reef.title, (SELECT count(*) FROM links l JOIN permitted p ON p.atom_id = l.atom_id), 0::bigint, NULL::text
FROM reef"#;

/// `$1` graph, `$2` node: the node read DTO's title and readable atom count,
/// when the node is permitted (`GET /v1/nodes/{id}`).
pub const NODE_ADMISSION_SQL: &str = r#"WITH reef AS MATERIALIZED (
 SELECT n.graph_id, n.node_id FROM nodes n
 WHERE n.graph_id = $1 AND n.node_id = $2 AND library_node_permitted(n.graph_id,n.node_id)
), links AS MATERIALIZED (
 SELECT l.atom_id FROM reef JOIN node_atoms l ON l.node_id = reef.node_id
), permitted AS MATERIALIZED (
 SELECT p.atom_id FROM reef
 CROSS JOIN LATERAL (SELECT ARRAY(SELECT atom_id FROM links) AS ids) i
 CROSS JOIN LATERAL library_permitted_atoms(reef.graph_id, CASE WHEN cardinality(i.ids) > 10000 THEN NULL ELSE i.ids END) p
)
SELECT library_node_title(reef.graph_id,reef.node_id), (SELECT count(*) FROM links l JOIN permitted p ON p.atom_id = l.atom_id)
FROM reef"#;

/// Legacy unscoped reef list (`Store::list_nodes`): `$1` graph, `$2` limit,
/// `$3` offset. The order needs every permitted node's readable atom count,
/// so the counts come from the graph's whole permitted set, hashed once.
pub const LIST_NODES_SQL: &str = r#"WITH permitted AS MATERIALIZED (
 SELECT p.atom_id FROM library_permitted_atoms($1, NULL::uuid[]) p
), counts AS MATERIALIZED (
 SELECT l.node_id, count(*) AS n FROM nodes cn JOIN node_atoms l ON l.node_id = cn.node_id
 JOIN permitted p ON p.atom_id = l.atom_id
 WHERE cn.graph_id = $1 GROUP BY l.node_id
)
SELECT n.node_id, n.type, COALESCE(library_node_title(n.graph_id,n.node_id),''), n.scope, n.created_at, n.updated_at, COALESCE(c.n, 0) AS atom_count
FROM nodes n LEFT JOIN counts c ON c.node_id = n.node_id
WHERE n.graph_id = $1 AND library_node_permitted(n.graph_id,n.node_id)
ORDER BY CASE n.type WHEN 'entity' THEN 0 WHEN 'thread' THEN 1
         WHEN 'decision' THEN 2 WHEN 'reference' THEN 3 ELSE 4 END,
         atom_count DESC, n.updated_at DESC, n.node_id ASC
LIMIT $2 OFFSET $3"#;

/// `LIST_NODES_SQL` for one node type: `$1` graph, `$2` type, `$3` limit, `$4` offset.
pub const LIST_NODES_OF_TYPE_SQL: &str = r#"WITH permitted AS MATERIALIZED (
 SELECT p.atom_id FROM library_permitted_atoms($1, NULL::uuid[]) p
), counts AS MATERIALIZED (
 SELECT l.node_id, count(*) AS n FROM nodes cn JOIN node_atoms l ON l.node_id = cn.node_id
 JOIN permitted p ON p.atom_id = l.atom_id
 WHERE cn.graph_id = $1 AND cn.type = $2 GROUP BY l.node_id
)
SELECT n.node_id, n.type, COALESCE(library_node_title(n.graph_id,n.node_id),''), n.scope, n.created_at, n.updated_at, COALESCE(c.n, 0) AS atom_count
FROM nodes n LEFT JOIN counts c ON c.node_id = n.node_id
WHERE n.graph_id = $1 AND library_node_permitted(n.graph_id,n.node_id)
  AND n.type = $2
ORDER BY atom_count DESC, n.updated_at DESC, n.node_id ASC
LIMIT $3 OFFSET $4"#;

/// The statements above and the library ones that compute a verdict, for
/// the plan check. `library::TOPICS_COUNT_SQL` computes none (it inlines the
/// title predicate, see there), so `p7_read_boundary_pg` checks it apart.
pub const STATEMENTS: &[(&str, &str)] = &[
    ("conversation_page", CONVERSATION_PAGE_SQL),
    ("query_history_page", QUERY_HISTORY_PAGE_SQL),
    ("query_history_graph_page", QUERY_HISTORY_GRAPH_PAGE_SQL),
    ("conversation_search", CONVERSATION_SEARCH_SQL),
    ("node_titles", NODE_TITLES_SQL),
    ("node_titles_with_counts", NODE_TITLES_WITH_COUNTS_SQL),
    ("node_digest", NODE_DIGEST_SQL),
    ("node_digest_context", NODE_DIGEST_CONTEXT_SQL),
    ("node_admission", NODE_ADMISSION_SQL),
    ("library_topics_page", super::library::TOPICS_PAGE_SQL),
    ("library_lineage", super::library::LINEAGE_SQL),
    ("library_decisions", super::library::DECISIONS_SQL),
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The SQL spells the threshold as a literal; keep it equal to the constant.
    #[test]
    fn permitted_ids_max_matches_sql() {
        let literal = format!("cardinality(i.ids) > {PERMITTED_IDS_MAX} THEN NULL");
        for sql in [
            NODE_TITLES_WITH_COUNTS_SQL,
            NODE_DIGEST_CONTEXT_SQL,
            NODE_ADMISSION_SQL,
            crate::store::library::TOPICS_PAGE_SQL,
            crate::source_evidence::NODE_EVIDENCE_COUNTS_SQL,
        ] {
            assert!(sql.contains(&literal), "{sql}");
            assert!(!sql.contains("library_atom_permitted"), "{sql}");
        }
    }
}
