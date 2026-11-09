//! The Library's per-node evidence definition, written once.
//!
//! A reef's evidence metrics (`source_count`,
//! `unknown_independence_record_count`, `atom_count`, `current_atom_count`,
//! `unmapped_legacy_count`, `admitted`) are decided by [`EVIDENCE_CTES`]: the
//! chain from a scope's permitted chunk atoms and retained outcomes to one
//! metrics row per node. Two instantiations read it, and nothing else defines
//! these metrics:
//!
//! 1. **The live read.** `library_read::scope_cte` binds the chain to the
//!    request's scope parameters (`$2`..`$6`) over the scope-wide input sets
//!    it builds first. Its text is byte-for-byte what it was before the chain
//!    moved here (`search_sql_is_byte_identical_to_main` pins that).
//! 2. **The node-keyed read** ([`NODE_EVIDENCE_SQL`],
//!    [`NODE_ITEM_EVIDENCE_SQL`], [`node_evidence_ctes`]). The same chain, unfiltered,
//!    over input sets probed from a given node set: `node_atoms` and
//!    `node_semantic_atoms` by node, `library_permitted_atoms(g, atom_ids)`
//!    (the id-list branch) for those nodes' atoms only. This is what the
//!    Library read model's refresher, its backfill and its read overlay run
//!    (`library-read-model-design.md`, "One definition, three users").
//!
//! The input sets each instantiation supplies, by name and the columns the
//! chain reads:
//! - `scope_graph_chunks(graph_id, atom_id, source_version_id)`
//! - `scope_graph_versions(graph_id, version_id, source_item_id,
//!   is_legacy_baseline, source_metadata, origin)`
//! - `scoped_permitted_atoms(graph_id, atom_id, unmapped)`
//! - `scope_sources(graph_id, source_item_id, is_legacy_baseline,
//!   source_metadata)`: the admitted items, with the selected version's
//!   independence facts
//! - `retained_scope_atoms(graph_id, semantic_atom_id, source_item_id,
//!   source_version_is_current)` and `scope_atoms(graph_id, semantic_atom_id)`
//!
//! A node's metrics read only that node's links, so computing them for a
//! subset of nodes from the subset's own links gives the same rows the
//! whole-scope read gives for those nodes. The pg suite
//! (`backend/tests/library_read_pg.rs`) proves the two instantiations agree
//! row for row in both outcome modes.
//!
//! Permission: the chain inherits the caller's RLS through
//! `library_permitted_atoms` (SECURITY INVOKER). Admission also depends on the
//! account and effective correction visibility. Stored rows require the same
//! access class (`library_evidence_class`: account, no history floor,
//! correction visibility, the reviewed RLS contract digest), not the role.

use std::sync::LazyLock;

/// Bump whenever the stored evidence semantics change. Future readers must
/// fall back to the live definition while a graph has another version.
///
/// Independent of the listing grain's `read_model::DEFINITION_VERSION`: the
/// listing program arriving at 3 changed nothing about node evidence, and
/// renumbering this would make every certified graph rebuild its node
/// evidence on deploy, listing program on or off.
pub const READ_MODEL_VERSION: i32 = 2;

/// The evidence chain. Placeholders:
/// - `__UNFILTERED__`: true when no origin, item, version or collection
///   filter applies. Unmapped legacy links admit a node only then.
/// - `__SELECTED_VERSION__`: whether chunk row `x` is the selected version.
/// - `__LINK_NODE__`: whether link row `l` belongs to a node being computed.
/// - `__CHUNK_VERSIONS__`, `__LEGACY_LINKS__`, `__SEMANTIC_LINKS__`:
///   the keyed input relations, chosen to retain base-table statistics where
///   the planner could otherwise join materialized sets on graph alone.
///
/// The comments are part of the live statement's text, kept as they were.
pub(crate) const EVIDENCE_CTES: &str = r#"scoped_chunk_atoms AS MATERIALIZED (
 SELECT c.graph_id,c.atom_id,v.source_item_id,v.version_id,
  NOT v.is_legacy_baseline AND v.source_metadata->>'independence' IS DISTINCT FROM 'derived_copy' independent,
  v.origin->>'transport'='synthetic' OR v.source_metadata->'synthetic_demo'='true'::jsonb synthetic
 FROM scoped_permitted_atoms p JOIN scope_graph_chunks c ON c.graph_id=p.graph_id AND c.atom_id=p.atom_id
 JOIN __CHUNK_VERSIONS__ v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
 WHERE NOT p.unmapped
), scoped_legacy_links AS MATERIALIZED (
 -- The scope's legacy node links, read from node_atoms once at the (node,
 -- item) grain both readers below use: `selected` is whether any chunk
 -- version is the selected one ($4), the flags whether any version carries
 -- them, which is what each reader asked of the version-grain rows. Chunk
 -- rows multiply by version and links by chunk: on the 2026-10-23 org tenant
 -- the 41k chunk rows of a 2.9k-item ocean made 1.0M link rows, which each
 -- reader materialised again and the node page rescanned per row. Reducing
 -- the chunk rows per atom first keeps the join at one row per link.
 SELECT x.graph_id,l.node_id,x.source_item_id,bool_or(x.selected) selected,bool_or(x.independent) independent,bool_or(x.synthetic) synthetic
 FROM (SELECT x.graph_id,x.atom_id,x.source_item_id,bool_or(__SELECTED_VERSION__) selected,bool_or(x.independent) independent,bool_or(x.synthetic) synthetic
  FROM scoped_chunk_atoms x GROUP BY 1,2,3) x
 JOIN __LEGACY_LINKS__ l ON l.atom_id=x.atom_id__LEGACY_JOIN_GRAPH__ AND (__LINK_NODE__)
 GROUP BY 1,2,3
), scoped_semantic_links AS MATERIALIZED (
 -- The scope's retained outcomes joined to their node links once: the
 -- source and outcome readers below both walked node_semantic_atoms for
 -- the same outcomes (0.25 s each on the 2026-10-23 org tenant).
 -- A selected node ($5) is the only node any reader of this set or the
 -- legacy set below projects or counts, so both carry its links alone.
 SELECT l.graph_id,l.node_id,a.semantic_atom_id,a.source_item_id,a.source_version_is_current__SEMANTIC_COLUMNS__ FROM retained_scope_atoms a
 JOIN __SEMANTIC_LINKS__ l ON l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id
 WHERE (__LINK_NODE__)
), scoped_node_sources AS MATERIALIZED (
 SELECT l.graph_id,l.node_id,l.source_item_id FROM scoped_semantic_links l
 UNION
 -- Every join edge here carries a selective key. Materialized sets have no column
 -- statistics, and a graph_id-only edge between two of them is a within-graph cross
 -- product the planner prices at one row (32M rows on the 5k-item fixture).
 SELECT x.graph_id,x.node_id,x.source_item_id FROM scoped_legacy_links x
 JOIN scope_sources s ON s.graph_id=x.graph_id AND s.source_item_id=x.source_item_id
 WHERE x.selected
), scoped_node_atoms AS MATERIALIZED (
 -- `scope_atoms` is the retained set or its served subset (the same items'
 -- versions either way), so its links are the retained links it keeps. An
 -- outcome has one version, so the current flag adds no rows.
 -- Joined on the distinct outcome keys (a hash on both PostgreSQL 16 and
 -- 18): as an EXISTS, 16 rescanned scope_atoms per link row.
 SELECT DISTINCT l.graph_id,l.node_id,l.semantic_atom_id,l.source_version_is_current FROM scoped_semantic_links l
 __SCOPE_ATOMS_JOIN__
), scoped_unmapped_legacy AS MATERIALIZED (
 SELECT p.graph_id,l.node_id,count(*) unmapped_count FROM scoped_permitted_atoms p JOIN __LEGACY_LINKS__ l ON l.atom_id=p.atom_id__UNMAPPED_JOIN_GRAPH__
 WHERE p.unmapped AND (__LINK_NODE__)
  AND ((__UNFILTERED__)
   OR (p.graph_id,l.node_id) IN (SELECT graph_id,node_id FROM scoped_node_sources))
 GROUP BY 1,2
), scope_node_metrics AS MATERIALIZED (
 -- Count each admitted evidence set once per key. Joining a materialized
 -- legacy set back to an underestimated node set can otherwise rescan every
 -- legacy row for every node in a whole-ocean scope.
 -- `current_atom_count` is the node's outcomes from served versions, counted
 -- here with the rest: as a per-row subquery it rehashed retained_scope_atoms
 -- for every page node (11 ms a row on the 2026-10-23 org tenant).
 SELECT graph_id,node_id,sum(source_count)::bigint source_count,
  sum(unknown_independence_record_count)::bigint unknown_independence_record_count,
  sum(atom_count)::bigint atom_count,sum(current_atom_count)::bigint current_atom_count,
  sum(unmapped_legacy_count)::bigint unmapped_legacy_count,
  bool_or(admitted) admitted
 FROM (
  -- Each item's independence is decided once (`s`), not per link row.
  SELECT links.graph_id,links.node_id,
   count(*) FILTER (WHERE NOT s.unknown_independence) source_count,
   count(*) FILTER (WHERE s.unknown_independence) unknown_independence_record_count,
   0::bigint atom_count,0::bigint current_atom_count,0::bigint unmapped_legacy_count,true admitted
  FROM scoped_node_sources links JOIN (SELECT graph_id,source_item_id,COALESCE(is_legacy_baseline,false) OR COALESCE(source_metadata->>'independence'='derived_copy',false) unknown_independence FROM scope_sources) s
   ON s.graph_id=links.graph_id AND s.source_item_id=links.source_item_id
  GROUP BY 1,2
  UNION ALL
  SELECT graph_id,node_id,0,0,count(*),count(*) FILTER (WHERE source_version_is_current),0,false FROM scoped_node_atoms GROUP BY 1,2
  UNION ALL
  SELECT graph_id,node_id,0,0,0,0,unmapped_count,
   __UNFILTERED__
  FROM scoped_unmapped_legacy
 ) evidence GROUP BY 1,2
)"#;

/// What [`EVIDENCE_CTES`]' placeholders mean for one caller.
pub(crate) struct EvidenceBinds<'a> {
    pub unfiltered: &'a str,
    pub selected_version: &'a str,
    pub chunk_versions: &'a str,
    pub legacy_links: &'a str,
    pub semantic_links: &'a str,
    pub link_node: &'a str,
    pub semantic_columns: &'a str,
    pub scope_atoms_join: &'a str,
    pub legacy_join_graph: &'a str,
    pub unmapped_join_graph: &'a str,
}

/// The live read's binds: the request's scope parameters.
pub(crate) const SCOPE_BINDS: EvidenceBinds<'static> = EvidenceBinds {
    unfiltered:
        "$2::jsonb='{}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL",
    selected_version: "$4::uuid IS NULL OR x.version_id=$4",
    link_node: "$5::uuid IS NULL OR l.node_id=$5",
    chunk_versions: "scope_graph_versions",
    legacy_links: "node_atoms",
    semantic_links: "node_semantic_atoms",
    semantic_columns: "",
    legacy_join_graph: "",
    unmapped_join_graph: "",
    scope_atoms_join: "JOIN (SELECT DISTINCT a.graph_id,a.semantic_atom_id FROM scope_atoms a) k ON k.semantic_atom_id=l.semantic_atom_id AND k.graph_id=l.graph_id",
};

/// The node-keyed read's binds: no filter, every version selected (the item's
/// current one is what independence reads), links of the computed nodes.
/// `node_id` is the `nodes` primary key, so it names the node in any graph.
const NODE_BINDS: EvidenceBinds<'static> = EvidenceBinds {
    unfiltered: "true",
    selected_version: "true",
    // Both keyed link relations already originate only from evidence_nodes.
    link_node: "true",
    // Base-table version statistics prevent a graph-only join between
    // the two low-cardinality materialized sets before the version key.
    chunk_versions: "LATERAL (SELECT v.graph_id,v.version_id,v.source_item_id,v.is_legacy_baseline,v.source_metadata,v.origin FROM source_versions v WHERE v.version_id=c.source_version_id AND v.graph_id=c.graph_id OFFSET 0)",
    // Joining the graph-wide inverse edge first multiplies hub atoms by
    // unrelated nodes; this relation already contains only requested links.
    legacy_links: "evidence_legacy_links",
    semantic_links: "evidence_semantic_links",
    semantic_columns: ",a.entities,a.captured_at,a.semantic_independent_version,a.semantic_synthetic",
    legacy_join_graph: " AND l.graph_id=x.graph_id",
    unmapped_join_graph: " AND l.graph_id=p.graph_id",
    // Every node-keyed outcome is retained: scoped_semantic_links already
    // joined that exact set, so rejoining its keys cannot remove any row.
    scope_atoms_join: "",
};

pub(crate) fn evidence_ctes(binds: &EvidenceBinds) -> String {
    EVIDENCE_CTES
        .replace("__UNFILTERED__", binds.unfiltered)
        .replace("__SELECTED_VERSION__", binds.selected_version)
        .replace("__LINK_NODE__", binds.link_node)
        .replace("__CHUNK_VERSIONS__", binds.chunk_versions)
        .replace("__LEGACY_LINKS__", binds.legacy_links)
        .replace("__SEMANTIC_LINKS__", binds.semantic_links)
        .replace("__SEMANTIC_COLUMNS__", binds.semantic_columns)
        .replace("__SCOPE_ATOMS_JOIN__", binds.scope_atoms_join)
        .replace("__LEGACY_JOIN_GRAPH__", binds.legacy_join_graph)
        .replace("__UNMAPPED_JOIN_GRAPH__", binds.unmapped_join_graph)
}

/// The node-keyed input sets, in the shapes the live read builds them, but
/// probed from `evidence_nodes(graph_id, node_id)` instead of scanned per
/// graph. Unfiltered: every permitted item, each read at its current version.
/// `__SERVED_VERSIONS__` is the live read's served-version rule, verbatim.
const NODE_INPUT_CTES: &str = r#"evidence_node_rows AS MATERIALIZED (
 SELECT e.graph_id,e.node_id,n.title,lower(n.title) title_lower,substring(n.title FOR 256) title256,
  lower(substring(n.title FOR 256)) title256_lower,n.owner_id,n.body->>'synthetic_hub' synthetic_hub,
  n.type,n.entity_type_sticky,n.entity_type
 FROM evidence_nodes e CROSS JOIN LATERAL (SELECT n.title,n.owner_id,n.body,n.type,n.entity_type_sticky,n.entity_type
  FROM nodes n WHERE n.graph_id=e.graph_id AND n.node_id=e.node_id OFFSET 0) n
), evidence_legacy_links AS MATERIALIZED (
 -- Fence the node probe: a graph-wide atom-to-link join fans out to links
 -- of unrelated nodes before its node filter can discard them.
 SELECT e.graph_id,e.node_id,l.atom_id FROM evidence_nodes e
 CROSS JOIN LATERAL (SELECT l.atom_id FROM node_atoms l WHERE l.node_id=e.node_id OFFSET 0) l
), evidence_semantic_links AS MATERIALIZED (
 SELECT e.graph_id,e.node_id,l.semantic_atom_id FROM evidence_nodes e
 CROSS JOIN LATERAL (SELECT l.semantic_atom_id FROM node_semantic_atoms l
  WHERE l.node_id=e.node_id AND l.graph_id=e.graph_id OFFSET 0) l
), evidence_atoms AS MATERIALIZED (
 -- The nodes' legacy atoms, by node (node_atoms has no graph; an atom
 -- counts only in its node's graph, which library_permitted_atoms checks).
 SELECT DISTINCT graph_id,atom_id FROM evidence_legacy_links
), scope_graph_chunks AS MATERIALIZED (
 SELECT c.graph_id,c.atom_id,c.source_version_id FROM evidence_atoms a
 CROSS JOIN LATERAL (SELECT c.graph_id,c.atom_id,c.source_version_id FROM source_version_chunks c
  WHERE c.atom_id=a.atom_id AND c.graph_id=a.graph_id OFFSET 0) c
), scope_graph_versions AS MATERIALIZED (
 SELECT v.graph_id,v.version_id,v.source_item_id,v.is_legacy_baseline,v.source_metadata,v.origin
 FROM (SELECT DISTINCT graph_id,source_version_id FROM scope_graph_chunks) c
 CROSS JOIN LATERAL (SELECT v.graph_id,v.version_id,v.source_item_id,v.is_legacy_baseline,v.source_metadata,v.origin
  FROM source_versions v WHERE v.graph_id=c.graph_id AND v.version_id=c.source_version_id OFFSET 0) v
), scoped_permitted_atoms AS MATERIALIZED (
 SELECT g.graph_id,p.atom_id,NOT EXISTS(SELECT 1 FROM scope_graph_chunks c WHERE c.graph_id=g.graph_id AND c.atom_id=p.atom_id) unmapped
 FROM (SELECT DISTINCT graph_id FROM evidence_nodes) g
 CROSS JOIN LATERAL library_evidence_permitted_atoms(g.graph_id,ARRAY(SELECT a.atom_id FROM evidence_atoms a WHERE a.graph_id=g.graph_id)) p
), evidence_outcomes AS MATERIALIZED (
 -- The inner distinct key plus both primary-key lookups already yields
 -- at most one row per outcome; avoid sorting wide entities a second time.
 SELECT a.graph_id,a.semantic_atom_id,a.source_version_id,v.source_item_id,a.entities,v.captured_at,
  NOT v.is_legacy_baseline AND v.source_metadata->>'independence' IS DISTINCT FROM 'derived_copy' semantic_independent_version,
  v.origin->>'transport'='synthetic' OR v.source_metadata->'synthetic_demo'='true'::jsonb semantic_synthetic
 FROM (SELECT DISTINCT graph_id,semantic_atom_id FROM evidence_semantic_links) l
 CROSS JOIN LATERAL (SELECT a.graph_id,a.semantic_atom_id,a.source_version_id,a.entities FROM semantic_atoms a
  WHERE a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id OFFSET 0) a
 CROSS JOIN LATERAL (SELECT v.source_item_id,v.captured_at,v.is_legacy_baseline,v.source_metadata,v.origin FROM source_versions v
  WHERE v.version_id=a.source_version_id AND v.graph_id=a.graph_id OFFSET 0) v
), scope_sources AS MATERIALIZED (
 -- The permitted items any of the nodes' links reach, at their current
 -- version: the live read's `scope_sources` with no filter, restricted to
 -- the items these nodes can count.
 SELECT s.graph_id,s.source_item_id,s.current_version_id,v.is_legacy_baseline,v.source_metadata
 FROM (SELECT graph_id,source_item_id FROM evidence_outcomes UNION SELECT graph_id,source_item_id FROM scope_graph_versions) k
 CROSS JOIN LATERAL (SELECT s.graph_id,s.source_item_id,s.current_version_id FROM source_items s
  WHERE s.graph_id=k.graph_id AND s.source_item_id=k.source_item_id AND s.access_state='permitted' OFFSET 0) s
 LEFT JOIN LATERAL (SELECT v.is_legacy_baseline,v.source_metadata FROM source_versions v
  WHERE v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.version_id=s.current_version_id OFFSET 0) v ON true
), scope_served_versions AS MATERIALIZED (
 __SERVED_VERSIONS__
), retained_scope_atoms AS MATERIALIZED (
 -- Every retained outcome of the nodes whose item is admitted, with whether
 -- its version is the one the item serves.
 SELECT o.graph_id,o.semantic_atom_id,o.source_item_id,o.entities,o.captured_at,
  o.semantic_independent_version,o.semantic_synthetic,
  COALESCE(o.source_version_id=COALESCE(served.version_id,s.current_version_id),false) source_version_is_current
 FROM evidence_outcomes o JOIN scope_sources s ON s.graph_id=o.graph_id AND s.source_item_id=o.source_item_id
 LEFT JOIN scope_served_versions served ON served.graph_id=s.graph_id AND served.source_item_id=s.source_item_id
), scope_atoms AS (
 -- Retained mode: the served counts are `current_atom_count`, which in
 -- retained mode counts exactly the served-version outcomes.
 SELECT graph_id,semantic_atom_id FROM retained_scope_atoms
)"#;

/// Per (node, item): the stored link grain. Unfiltered; a filtered read sums
/// these over the items its filter admits.
/// - `semantic_link`: a retained outcome of the item links the node.
/// - `legacy_link`: a mapped, permitted chunk atom of the item links the node.
/// - `legacy_independent_version`, `legacy_synthetic`: whether any of those
///   chunks' versions is independent / synthetic (wider context reads them).
/// - `atom_count_retained`, `atom_count_served`: the node's outcomes of the
///   item, retained and served.
/// - `project_mention`: an outcome of the item names the node, by its title,
///   as a project.
/// - `semantic_*`: unfiltered historical facts from the actually linked
///   retained outcomes' versions, independent of the current item pointer.
/// - `entity_type_*`: latest raw-title256 matching outcome tuple, retaining
///   nullable types and explicit ambiguity of that tuple only.
const NODE_ITEM_CTES: &str = r#"evidence_link_mentions AS MATERIALIZED (
 -- Expand each admitted linked outcome once for both full-title support and
 -- the deliberately truncated project-mention rule.
 SELECT l.graph_id,l.node_id,l.source_item_id,lower(e->>'mention') mention,e->>'type'='project' project,
  e->>'type' entity_type,l.captured_at,l.semantic_atom_id
 FROM scoped_semantic_links l
 CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(l.entities)='array' THEN l.entities ELSE '[]'::jsonb END) e
), evidence_project_mentions AS MATERIALIZED (
 SELECT DISTINCT l.graph_id,l.node_id,l.source_item_id FROM evidence_link_mentions l
 JOIN evidence_node_rows n ON n.node_id=l.node_id AND n.graph_id=l.graph_id
 WHERE l.project AND l.mention=n.title256_lower
), evidence_entity_type_tuples AS MATERIALIZED (
 -- Canonical entity matching uses the raw title256, independently of the
 -- full-title visibility witness. NULL is a distinct possible type: an
 -- all-NULL tuple is unambiguous, while NULL plus text is ambiguous.
 SELECT l.graph_id,l.node_id,l.source_item_id,l.captured_at,l.semantic_atom_id,
  min(l.entity_type) entity_type_value,
  count(DISTINCT l.entity_type)+CASE WHEN bool_or(l.entity_type IS NULL) THEN 1 ELSE 0 END>1 entity_type_ambiguous
 FROM evidence_link_mentions l JOIN evidence_node_rows n ON n.graph_id=l.graph_id AND n.node_id=l.node_id
 WHERE l.mention=n.title256_lower GROUP BY 1,2,3,4,5
), evidence_entity_type_winners AS MATERIALIZED (
 -- Only ambiguity of the latest tuple matters; never choose a type to break
 -- a tied outcome or substitute an older non-NULL value.
 SELECT DISTINCT ON (graph_id,node_id,source_item_id) graph_id,node_id,source_item_id,
  captured_at,semantic_atom_id,CASE WHEN NOT entity_type_ambiguous THEN entity_type_value END entity_type_value,
  entity_type_ambiguous
 FROM evidence_entity_type_tuples
 ORDER BY graph_id,node_id,source_item_id,captured_at DESC,semantic_atom_id DESC
), evidence_item_links AS MATERIALIZED (
 SELECT graph_id,node_id,source_item_id,bool_or(semantic_link) semantic_link,bool_or(legacy_link) legacy_link,
  bool_or(legacy_independent_version) legacy_independent_version,COALESCE(bool_or(legacy_synthetic),false) legacy_synthetic,
  sum(atom_count_retained)::bigint atom_count_retained,sum(atom_count_served)::bigint atom_count_served,
  bool_or(project_mention) project_mention,
  COALESCE(bool_or(semantic_independent_version),false) semantic_independent_version,
  COALESCE(bool_or(semantic_synthetic),false) semantic_synthetic,max(semantic_latest_captured_at) semantic_latest_captured_at
 FROM (
  SELECT graph_id,node_id,source_item_id,true semantic_link,false legacy_link,false legacy_independent_version,false legacy_synthetic,
   count(DISTINCT semantic_atom_id) atom_count_retained,count(DISTINCT semantic_atom_id) FILTER (WHERE source_version_is_current) atom_count_served,false project_mention,
   bool_or(semantic_independent_version) semantic_independent_version,bool_or(semantic_synthetic) semantic_synthetic,
   max(captured_at) semantic_latest_captured_at
  FROM scoped_semantic_links GROUP BY 1,2,3
  UNION ALL
  SELECT x.graph_id,x.node_id,x.source_item_id,false,true,x.independent,x.synthetic,0,0,false,false,false,NULL::timestamptz FROM scoped_legacy_links x
  JOIN scope_sources s ON s.graph_id=x.graph_id AND s.source_item_id=x.source_item_id
  UNION ALL
  SELECT graph_id,node_id,source_item_id,false,false,false,false,0,0,true,false,false,NULL::timestamptz FROM evidence_project_mentions
 ) k GROUP BY 1,2,3
), node_item_evidence AS MATERIALIZED (
 SELECT k.*,e.semantic_atom_id IS NOT NULL entity_type_has_match,e.entity_type_value,
  e.captured_at entity_type_captured_at,e.semantic_atom_id entity_type_semantic_atom_id,
  COALESCE(e.entity_type_ambiguous,false) entity_type_ambiguous
 FROM evidence_item_links k LEFT JOIN evidence_entity_type_winners e
  ON e.graph_id=k.graph_id AND e.node_id=k.node_id AND e.source_item_id=k.source_item_id
)"#;

/// Per node, for every node of `evidence_nodes` (zeros when it has no
/// evidence):
/// - `source_count`, `unknown_independence_record_count`,
///   `unmapped_legacy_count`: unfiltered, as the live read counts them.
/// - `atom_count_retained`, `atom_count_served`: `atom_count` in the two
///   `outcome_versions` modes; `current_atom_count` is `atom_count_served` in
///   both.
/// - `admitted_unfiltered`: the node is in an unfiltered scope's node set.
/// - `has_source_link`: a source (semantic or legacy) link admits it.
/// - `title256`: the stored title the live read matches a query against.
/// - `title_visible`: `library_node_title` is not null. Evaluated for admitted
///   nodes only (no reader shows another), false otherwise.
/// - `project_mention`: some unfiltered retained outcome names it as a project.
/// - `stats_admitted`: current workspace-card reef admission, using its
///   extraction-generation view (distinct from Library served-version mode).
/// - `work_eligible`: the `prominent_work` predicate apart from
///   `source_count > 0`, which a reader applies to the counts it reads.
// Canonical library_node_title (0137/0157 and control twins), evaluated for
// the batch as keyed sets. Titles match the FULL stored title; title256 below
// is presentation/search text, not the evidence identity. Keep the correction
// branch SECURITY INVOKER: its RLS context is part of cache admission.
const NODE_ROWS_CTE: &str = r#"evidence_title_atoms AS MATERIALIZED (
 -- A hub can reuse one atom for thousands of nodes. Read/lower its payload
 -- once per permitted atom, preserving the canonical permission function.
 -- history_floor: scoped_permitted_atoms is library_permitted_atoms, which floors.
 SELECT p.graph_id,p.atom_id,lower(COALESCE(a.content_inline,'')) content_lower,
  ARRAY(SELECT lower(participant) FROM unnest(a.participants) participant) participants_lower
 FROM scoped_permitted_atoms p JOIN atoms a ON a.graph_id=p.graph_id AND a.atom_id=p.atom_id
), node_title_support AS MATERIALIZED (
 SELECT l.graph_id,l.node_id FROM evidence_link_mentions l
 JOIN evidence_node_rows n ON n.graph_id=l.graph_id AND n.node_id=l.node_id
 WHERE l.mention=n.title_lower
 UNION
 SELECT l.graph_id,l.node_id FROM evidence_legacy_links l
 JOIN evidence_title_atoms a ON a.graph_id=l.graph_id AND a.atom_id=l.atom_id
 JOIN evidence_node_rows n ON n.graph_id=l.graph_id AND n.node_id=l.node_id
 WHERE n.title_lower<>'' AND (position(n.title_lower IN a.content_lower)>0
  OR n.title_lower=ANY(a.participants_lower))
 UNION
 SELECT n.graph_id,n.node_id FROM evidence_node_rows n
 WHERE EXISTS(SELECT 1 FROM reef_identity_corrections c WHERE c.graph_id=n.graph_id AND c.node_id=n.node_id
  AND c.state='active' AND c.label=n.title AND reef_identity_support_valid(c))
), evidence_stats_outcomes AS MATERIALIZED (
 SELECT g.graph_id,a.semantic_atom_id FROM (SELECT DISTINCT graph_id FROM evidence_nodes) g
 CROSS JOIN LATERAL library_evidence_served_outcomes(g.graph_id,
  ARRAY(SELECT o.semantic_atom_id FROM evidence_outcomes o WHERE o.graph_id=g.graph_id)) a
), evidence_stats_nodes AS MATERIALIZED (
 SELECT graph_id,node_id FROM scoped_legacy_links
 UNION
 SELECT l.graph_id,l.node_id FROM evidence_semantic_links l JOIN evidence_stats_outcomes a
  ON a.graph_id=l.graph_id AND a.semantic_atom_id=l.semantic_atom_id
), node_evidence_base AS MATERIALIZED (
 SELECT e.graph_id,e.node_id,
  COALESCE(m.source_count,0)::bigint source_count,
  COALESCE(m.unknown_independence_record_count,0)::bigint unknown_independence_record_count,
  COALESCE(m.atom_count,0)::bigint atom_count_retained,
  COALESCE(m.current_atom_count,0)::bigint atom_count_served,
  COALESCE(m.unmapped_legacy_count,0)::bigint unmapped_legacy_count,
  COALESCE(m.admitted,false) admitted_unfiltered,
  COALESCE(m.source_count+m.unknown_independence_record_count>0,false) has_source_link,
  n.title256,
  COALESCE(m.admitted,false) AND title.node_id IS NOT NULL title_visible,
  pm.node_id IS NOT NULL project_mention,
  n.owner_id IS NULL AND n.synthetic_hub IS DISTINCT FROM 'true'
   AND (n.type='topic' OR (n.entity_type_sticky AND n.entity_type='project') OR pm.node_id IS NOT NULL) work_eligible_before_title,
  n.type IS NOT NULL AND (stats.node_id IS NOT NULL OR COALESCE(m.unmapped_legacy_count,0)>0) stats_admitted
 FROM evidence_nodes e JOIN evidence_node_rows n ON n.node_id=e.node_id AND n.graph_id=e.graph_id
 LEFT JOIN scope_node_metrics m ON m.graph_id=e.graph_id AND m.node_id=e.node_id
 LEFT JOIN (SELECT DISTINCT graph_id,node_id FROM evidence_project_mentions) pm ON pm.graph_id=e.graph_id AND pm.node_id=e.node_id
 LEFT JOIN node_title_support title ON title.graph_id=e.graph_id AND title.node_id=e.node_id
 LEFT JOIN evidence_stats_nodes stats ON stats.graph_id=e.graph_id AND stats.node_id=e.node_id
), node_evidence AS MATERIALIZED (
 SELECT b.*,work_eligible_before_title AND title_visible work_eligible FROM node_evidence_base b
)"#;

/// The node-keyed definition's CTEs, ending in `node_item_evidence` and
/// `node_evidence`, over `nodes_sql`: a query returning the
/// `(graph_id, node_id)` pairs to compute. A caller appends its own
/// statement (a SELECT, or an INSERT … ON CONFLICT), or nests the whole
/// `WITH … SELECT` as a subquery: its names are then local to it, so it can
/// sit beside the live read's CTEs of the same names. `nodes_sql` must be
/// trusted SQL, with all request values supplied as bind parameters.
pub fn node_evidence_ctes(nodes_sql: &str) -> String {
    format!(
        "WITH evidence_nodes AS MATERIALIZED (\n {nodes_sql}\n), {}, {}, {}, {}",
        NODE_INPUT_CTES.replace("__SERVED_VERSIONS__", super::SERVED_VERSIONS_SQL),
        evidence_ctes(&NODE_BINDS),
        NODE_ITEM_CTES,
        NODE_ROWS_CTE,
    )
}

/// One graph's batch of nodes: `$1` graph id, `$2` node ids. Ids that are not
/// nodes of `$1` yield no row.
pub const NODE_BATCH_SQL: &str =
    "SELECT n.graph_id,n.node_id FROM nodes n WHERE n.graph_id=$1 AND n.node_id=ANY($2::uuid[])";

/// Column list of `node_evidence` rows, in order.
pub const NODE_EVIDENCE_COLUMNS: &str = "graph_id,node_id,source_count,unknown_independence_record_count,atom_count_retained,atom_count_served,unmapped_legacy_count,admitted_unfiltered,has_source_link,title256,title_visible,project_mention,work_eligible,stats_admitted";

/// Per-node evidence rows for one graph's batch (`$1` graph, `$2` node ids).
pub static NODE_EVIDENCE_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{} SELECT {NODE_EVIDENCE_COLUMNS} FROM node_evidence ORDER BY graph_id,node_id",
        node_evidence_ctes(NODE_BATCH_SQL)
    )
});

/// Column list of `node_item_evidence` rows, in storage/overlay order. Version
/// two appends historical semantic context and the nullable top entity tuple.
pub const NODE_ITEM_EVIDENCE_COLUMNS: &str = "graph_id,node_id,source_item_id,semantic_link,legacy_link,legacy_independent_version,legacy_synthetic,atom_count_retained,atom_count_served,project_mention,semantic_independent_version,semantic_synthetic,semantic_latest_captured_at,entity_type_has_match,entity_type_value,entity_type_captured_at,entity_type_semantic_atom_id,entity_type_ambiguous";

/// Per (node, item) link rows for one graph's batch (`$1` graph, `$2` node ids).
pub static NODE_ITEM_EVIDENCE_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{} SELECT {NODE_ITEM_EVIDENCE_COLUMNS} FROM node_item_evidence ORDER BY graph_id,node_id,source_item_id",
        node_evidence_ctes(NODE_BATCH_SQL)
    )
});
