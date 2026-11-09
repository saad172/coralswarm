//! Definition-3 listing inputs. Every dirty expansion is dominated by admission;
//! clean SQL never renders either canonical recomputation tree.
//!
//! Busy graphs (#1524 blocker 2, design §3.3 B-ii steps 5-8). The dirty
//! statement embeds the change set (`crate::library_changes`): admission
//! counts DISTINCT pending keys, never log rows, and only the keys that can
//! affect this read are overlaid.
//! * Items: an item out of the read's scope in both its old image (the
//!   published head) and its new image (the canonical header in this
//!   snapshot) keeps its stored row, which the read's own scope predicate
//!   then excludes, exactly as Live excludes its fresh row. Every relevant
//!   item is swapped for its fresh header and counts (`Counts` mode of the
//!   one canonical definition), so totals and facets aggregate over exact
//!   contributions (step 5).
//! * Fresh outcome ROWS are materialized only for relevant items in scope,
//!   and on the ranked whole-ocean page only for items that can reach it
//!   (step 6, the top-K merge bound below).
//! * Nodes: an unfiltered read overlays every dirty node (`admitted_unfiltered`
//!   can flip, so `node_count` and ranks may move). A filtered read overlays
//!   only a dirty node whose stored or current items meet the read's scope
//!   (step 7).
//! * A hub (more distinct dirty nodes than budgeted) or a big node (dirty
//!   nodes over the raw-link budget) serves the stored nodes for a bounded
//!   window, marked stale, while sources, outcomes, totals and facets stay
//!   exact (design §3.5, founder decisions 2026-10-28).
use super::super::{listing_facts, LibraryScope};
use crate::library_changes;

const HEADER_COLUMNS: &str = "graph_id,source_item_id,source_type,provider_namespace,source_id,origin,discovery_content_kind,current_version_id,selected_version_id,served_version_id,captured_at,title256,unknown_independence,raw_extraction_status,raw_next_start_char,raw_extraction_reason,retained_count,served_count";
const HEADER_RECORD: &str = "graph_id uuid,source_item_id uuid,source_type text,provider_namespace text,source_id text,origin jsonb,discovery_content_kind text,current_version_id uuid,selected_version_id uuid,served_version_id uuid,captured_at timestamptz,title256 text,unknown_independence boolean,raw_extraction_status text,raw_next_start_char bigint,raw_extraction_reason text,retained_count bigint,served_count bigint";
const OUTCOME_COLUMNS: &str = "graph_id,semantic_atom_id,source_version_id,source_item_id,occurred_at,captured_at,source_version_is_current";
const OUTCOME_RECORD: &str = "graph_id uuid,semantic_atom_id uuid,source_version_id uuid,source_item_id uuid,occurred_at timestamptz,captured_at timestamptz,source_version_is_current boolean";

/// The change set's binds in the read statement, gated on the preliminary
/// verdict: a read already refused (narrow, floored, not ready, another
/// class, a stale worker) extracts nothing, since `unnest(NULL)` is empty.
const CHANGE_BINDS: library_changes::Binds = library_changes::Binds {
    graphs: "(CASE WHEN (SELECT reason IS NULL FROM model_preliminary) THEN $1::uuid[] END)",
    origin: "$2",
    collection: "$6",
};

/// No origin filter and no collection: every published head is in scope.
/// (An item, version or node scope never reaches the Model: `narrow_scope`.)
pub(super) const UNFILTERED: &str = "($2::jsonb='{}'::jsonb AND $6::text IS NULL)";

pub(super) fn admission(clean: bool) -> String {
    let mut sql = format!(r#"WITH model_graphs AS MATERIALIZED (
 SELECT ids.id,g.account_id,s.ready_version,s.computed_account_id,s.computed_context,s.last_refreshed_at,
  l.ready_version listing_ready,l.definition_version,l.computed_account_id listing_account,l.computed_context listing_context,
  l.sources_complete,l.nodes_complete,l.aggregates_complete,l.sources_old_complete,l.epoch,l.node_count,
  -- Facts are complete only for the switch generation that captured them;
  -- with the switch off nothing maintains them, however ready they look.
  l.capture_since,(SELECT w.enabled_since FROM library_listing_switch w WHERE w.enabled) switch_since,
  library_evidence_shareable(ids.id) shareable,{reader_class} reader_class
 FROM (SELECT DISTINCT unnest($1::uuid[]) id) ids LEFT JOIN graphs g ON g.id=ids.id
 LEFT JOIN library_evidence_state s ON s.graph_id=ids.id
 LEFT JOIN library_listing_state l ON l.graph_id=ids.id
), model_pending AS MATERIALIZED (
 -- The head of each log per scope graph, one bounded index probe each and
 -- never a count: whether anything is pending, and how long the oldest
 -- probed change has waited (created_at is its writer's transaction start,
 -- at or before its commit). Budgets are decided on distinct keys below.
 SELECT g.id graph_id,g.last_refreshed_at,n.oldest node_oldest,LEAST(n.oldest,l.oldest) oldest_at
 FROM model_graphs g
 CROSS JOIN LATERAL (SELECT min(c.created_at) oldest FROM (SELECT c.created_at FROM library_evidence_changes c WHERE c.graph_id=g.id ORDER BY c.id LIMIT {probe}) c) n
 CROSS JOIN LATERAL (SELECT min(c.created_at) oldest FROM (SELECT c.created_at FROM library_listing_changes c WHERE c.graph_id=g.id ORDER BY c.id LIMIT {probe}) c) l
), model_preliminary AS MATERIALIZED (
 SELECT CASE WHEN $3::uuid IS NOT NULL OR $4::uuid IS NOT NULL OR $5::uuid IS NOT NULL THEN 'narrow_scope'
  -- A reader with a team-seat history floor on any scope graph reads Live,
  -- which applies the floor (crate::history_floor). Cached rows never do.
  WHEN EXISTS(SELECT 1 FROM model_graphs WHERE account_id IS NULL OR NOT COALESCE(shareable,false)) OR {floored} THEN 'permission_window'
  WHEN EXISTS(SELECT 1 FROM model_graphs WHERE ready_version IS DISTINCT FROM {node_version} OR listing_ready IS DISTINCT FROM 3
   OR definition_version IS DISTINCT FROM 3 OR sources_complete IS DISTINCT FROM true OR nodes_complete IS DISTINCT FROM true
   OR aggregates_complete IS DISTINCT FROM true OR sources_old_complete IS DISTINCT FROM true
   OR switch_since IS NULL OR capture_since IS DISTINCT FROM switch_since) THEN 'not_ready'
  -- Access class (crate::read_access): the reviewed RLS contract, then the
  -- reader's own class on both grains, whatever role computed or reads them.
  WHEN EXISTS(SELECT 1 FROM model_graphs WHERE {contract_changed}) THEN '{contract_reason}'
  WHEN EXISTS(SELECT 1 FROM model_graphs WHERE computed_account_id IS DISTINCT FROM account_id OR listing_account IS DISTINCT FROM account_id
   OR {class_mismatch}) THEN '{class_reason}'
  -- Worker heartbeat (design §3.3 step 8), not an age rule: the overlay is
  -- exact at any age. A graph whose oldest pending change has waited past
  -- the window with no refresh of the graph committed since it was logged
  -- has no worker draining it; its overlay would only grow.
  WHEN EXISTS(SELECT 1 FROM model_pending p WHERE p.oldest_at<statement_timestamp()-interval '{heartbeat}s'
   AND p.last_refreshed_at<p.oldest_at) THEN 'change_stale' END reason
)"#, probe=super::PENDING_PROBE_ROWS,heartbeat=super::WORKER_HEARTBEAT_SECS,node_version=super::super::evidence::READ_MODEL_VERSION,floored=crate::history_floor::floored_on_any("$1::uuid[]"),reader_class=crate::read_access::sql::reader_class("ids.id"),
        contract_changed=crate::read_access::sql::contract_changed("reader_class",&["computed_context","listing_context"]),
        class_mismatch=crate::read_access::sql::class_mismatch("reader_class",&["computed_context","listing_context"]),
        contract_reason=crate::read_access::CONTRACT_CHANGED,class_reason=crate::read_access::CLASS_MISMATCH);
    if clean {
        // Typed empties share the small page/rank renderer, not its dirty tree.
        sql.push_str(r#", model_dirty AS (SELECT NULL::uuid graph_id,NULL::uuid node_id WHERE false),
 model_dirty_items AS (SELECT NULL::uuid graph_id,NULL::uuid source_item_id WHERE false),
 model_hub AS (SELECT false hub),
 model_admission AS MATERIALIZED (SELECT reason FROM model_preliminary),
 model_clean_pending AS MATERIALIZED (
 SELECT EXISTS(SELECT 1 FROM model_pending WHERE oldest_at IS NOT NULL) dirty
)"#);
        return sql;
    }
    let in_scope_head = listing_facts::scope_predicate("h", "$2", "$6");
    sql.push_str(&format!(r#", {changes},
 model_dirty_inputs AS MATERIALIZED (
 -- The physical inputs of every dirty node, before relevance or any
 -- recomputation walks them (row security off, tenant 0208 / control 0393).
 SELECT CASE WHEN (SELECT reason FROM change_decision) IS NOT NULL OR NOT EXISTS(SELECT 1 FROM change_nodes) THEN true
  ELSE public.library_changes_raw_edges_safe(ARRAY(SELECT graph_id FROM change_graphs),ARRAY[]::bigint[],ARRAY(SELECT node_id FROM change_nodes),{raw_edges}) END safe
), model_hub AS MATERIALIZED (
 -- The node grain cannot be overlaid exactly within budget: more distinct
 -- dirty nodes than budgeted (one change fanned out too far, `dirty_hub`),
 -- or dirty nodes with more physical links than budgeted (one big node,
 -- `raw_edge_overflow`). Nodes are then served from the stored rows; items
 -- stay exact (founder decisions 2026-10-28). `why` is the reason the read
 -- falls back with once the window closes.
 SELECT h.why IS NOT NULL hub,h.why FROM (SELECT CASE
  WHEN (SELECT reason FROM change_decision)='dirty_hub' THEN 'dirty_hub'
  WHEN (SELECT reason FROM change_decision) IS NULL AND NOT (SELECT safe FROM model_dirty_inputs) THEN 'raw_edge_overflow' END why) h
), model_hub_since AS MATERIALIZED (
 -- The stale window starts at the oldest probed pending node-grain change.
 SELECT min(node_oldest) since FROM model_pending
), model_dirty_items AS MATERIALIZED (
 -- Only items in the read's scope in either image are overlaid (step 4).
 SELECT r.graph_id,r.source_item_id FROM change_relevant_items r
 WHERE (SELECT reason FROM change_decision) IS NULL OR (SELECT hub FROM model_hub)
), model_dirty_node_items AS MATERIALIZED (
 -- Filtered reads only (step 7): each dirty node's stored items and a
 -- superset of its current ones (every item its semantic and legacy links
 -- reach, before any permission gate). A node none of whose items is in
 -- scope has no filtered contribution, stored or fresh.
 SELECT n.graph_id,n.node_id,k.source_item_id FROM change_nodes n CROSS JOIN LATERAL (
  SELECT i.source_item_id FROM library_node_item_evidence i WHERE i.graph_id=n.graph_id AND i.node_id=n.node_id
  UNION ALL
  SELECT v.source_item_id FROM node_semantic_atoms l JOIN semantic_atoms a ON a.graph_id=l.graph_id AND a.semantic_atom_id=l.semantic_atom_id
  JOIN source_versions v ON v.graph_id=a.graph_id AND v.version_id=a.source_version_id WHERE l.graph_id=n.graph_id AND l.node_id=n.node_id
  UNION ALL
  SELECT v.source_item_id FROM node_atoms l JOIN source_version_chunks c ON c.graph_id=n.graph_id AND c.atom_id=l.atom_id
  JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id WHERE l.node_id=n.node_id
 ) k WHERE NOT {unfiltered} AND (SELECT reason FROM change_decision) IS NULL AND (SELECT safe FROM model_dirty_inputs)
), model_dirty AS MATERIALIZED (
 SELECT n.graph_id,n.node_id FROM change_nodes n
 WHERE {unfiltered} AND (SELECT reason FROM change_decision) IS NULL AND (SELECT safe FROM model_dirty_inputs)
 UNION
 -- An item is in this read's scope by its fresh header when it is a
 -- classified dirty item (an irrelevant one is in neither image), and by
 -- its published head otherwise: the item set of scope_sources.
 SELECT m.graph_id,m.node_id FROM model_dirty_node_items m
 LEFT JOIN change_relevance r ON r.graph_id=m.graph_id AND r.source_item_id=m.source_item_id
 LEFT JOIN library_listing_source_heads h ON r.source_item_id IS NULL AND h.graph_id=m.graph_id AND h.source_item_id=m.source_item_id
 WHERE CASE WHEN r.source_item_id IS NOT NULL THEN r.new_in_scope ELSE h.source_item_id IS NOT NULL AND ({in_scope_head}) END
), model_listing_versions AS MATERIALIZED (
 SELECT v.graph_id,v.version_id FROM model_dirty_items d CROSS JOIN LATERAL (
 SELECT v.graph_id,v.version_id FROM source_versions v WHERE v.graph_id=d.graph_id AND v.source_item_id=d.source_item_id LIMIT {inputs_plus}) v LIMIT {inputs_plus}
), model_listing_raw_outcomes AS MATERIALIZED (
 SELECT a.semantic_atom_id FROM model_listing_versions v CROSS JOIN LATERAL (
 SELECT a.semantic_atom_id FROM semantic_atoms a WHERE a.graph_id=v.graph_id AND a.source_version_id=v.version_id LIMIT {inputs_plus}) a LIMIT {inputs_plus}
), model_listing_old_outcomes AS MATERIALIZED (
 SELECT a.semantic_atom_id FROM model_dirty_items d CROSS JOIN LATERAL (
 SELECT a.semantic_atom_id FROM library_listing_outcomes a WHERE a.graph_id=d.graph_id AND a.source_item_id=d.source_item_id
 ORDER BY a.semantic_atom_id LIMIT {inputs_plus}) a LIMIT {inputs_plus}
), model_admission AS MATERIALIZED (
 SELECT COALESCE((SELECT reason FROM model_preliminary),CASE
  WHEN (SELECT reason FROM change_decision)<>'dirty_hub' THEN (SELECT reason FROM change_decision)
  -- A stale node window never hides a removal: an item deleted or no
  -- longer permitted (a published head with no fresh header) still counts
  -- in the stored node rows, so the read goes exact Live instead.
  WHEN (SELECT hub FROM model_hub) AND EXISTS(SELECT 1 FROM change_relevance r
   JOIN library_listing_source_heads h ON h.graph_id=r.graph_id AND h.source_item_id=r.source_item_id
   WHERE NOT EXISTS(SELECT 1 FROM change_fresh_headers f WHERE f.graph_id=r.graph_id AND f.source_item_id=r.source_item_id)) THEN (SELECT why FROM model_hub)
  WHEN (SELECT hub FROM model_hub) AND COALESCE((SELECT since FROM model_hub_since),'-infinity'::timestamptz)<statement_timestamp()-interval '{hub_secs}s' THEN (SELECT why FROM model_hub)
  WHEN (SELECT count(*) FROM model_listing_versions)>{inputs} OR (SELECT count(*) FROM model_listing_raw_outcomes)>{inputs}
   OR (SELECT count(*) FROM model_listing_old_outcomes)>{inputs} THEN 'dirty_item_input_overflow'
 END) reason
)"#, changes=library_changes::ctes(CHANGE_BINDS,super::change_budgets()),raw_edges=super::RAW_EDGE_LIMIT,unfiltered=UNFILTERED,
        hub_secs=super::HUB_STALE_SECS,inputs_plus=super::DIRTY_ITEM_INPUT_LIMIT+1,inputs=super::DIRTY_ITEM_INPUT_LIMIT));
    sql
}

/// Sort-key order of the outcome page as one comparable row: occurred_at
/// DESC NULLS LAST, then captured_at DESC (a NULL first, as DESC places it).
/// A larger row ranks earlier on the page.
fn outcome_rank_key(alias: &str) -> String {
    format!("({alias}.occurred_at IS NOT NULL,COALESCE({alias}.occurred_at,'-infinity'::timestamptz),COALESCE({alias}.captured_at,'infinity'::timestamptz))")
}

/// The listing inputs. `rank` is the ranked whole-ocean shape (empty query,
/// unfiltered), whose pages come from per-graph stored prefixes plus fresh
/// rows (`rank_pages`) and whose totals come from the heads (`totals`).
pub(super) fn inputs(clean: bool, scope: &LibraryScope, rank: bool) -> String {
    let (fresh, rows) = if clean {
        ("SELECT '[]'::jsonb value".to_owned(), "SELECT '[]'::jsonb value".to_owned())
    } else {
        (format!("{} SELECT COALESCE((SELECT jsonb_agg(to_jsonb(h)) FROM listing_source_facts h),'[]'::jsonb) value",
            listing_facts::canonical_ctes_in(listing_facts::ItemInput::Dirty, listing_facts::Mode::Counts)),
         format!("{} SELECT COALESCE((SELECT jsonb_agg(to_jsonb(o)) FROM listing_outcomes o),'[]'::jsonb) value",
            listing_facts::canonical_ctes_in(listing_facts::ItemInput::PageRows, listing_facts::Mode::Counts)))
    };
    let mode = if super::super::reads_served_versions_only(scope) { "AND o.source_version_is_current" } else { "" };
    let scope_where = listing_facts::scope_predicate("h", "$2", "$6");
    let fresh_in_scope = listing_facts::scope_predicate("f", "$2", "$6");
    // Step 6, the top-K merge bound. The page is the first $15 rows of
    // (clean rows) ∪ (fresh rows). θ is the clean page's $15-th row: a fresh
    // row ranking below θ has $15 clean rows ahead of it and cannot enter.
    // An item needs fresh rows only if one of its versions ranks at or above
    // θ on (occurred_at, captured_at), a superset of its rows that can (the
    // semantic atom id only breaks ties inside one such pair). With fewer
    // than $15 clean rows there is no θ and every in-scope item qualifies.
    let outcome = OUTCOME_COLUMNS.split(',').map(|c| format!("o.{c}")).collect::<Vec<_>>().join(",");
    let (threshold, reach) = match (rank, clean) {
        (false, _) => (String::new(), String::new()),
        // Clean: no fresh rows, so the clean page is the page.
        (true, true) => (format!("{}, ", clean_outcome_page(&outcome, mode)), String::new()),
        (true, false) => (
            format!(r#"{}, model_outcome_threshold AS MATERIALIZED (
 SELECT o.occurred_at,o.captured_at FROM model_clean_outcome_page o
 ORDER BY o.occurred_at DESC NULLS LAST,o.captured_at DESC,o.semantic_atom_id DESC OFFSET $15-1 LIMIT 1
), "#, clean_outcome_page(&outcome, mode)),
            format!(r#"
 -- history_floor: a floored reader never reaches the Model (permission_window);
 -- this probe only decides whose fresh rows to materialize and shows nothing.
 AND (NOT EXISTS(SELECT 1 FROM model_outcome_threshold) OR EXISTS(SELECT 1 FROM source_versions v CROSS JOIN model_outcome_threshold t
  WHERE v.graph_id=f.graph_id AND v.source_item_id=f.source_item_id AND {}>={}))"#, outcome_rank_key("v"), outcome_rank_key("t")),
        ),
    };
    format!(r#"model_listing_live AS MATERIALIZED ({fresh}),
 model_listing_fresh AS MATERIALIZED (
 SELECT h.* FROM model_listing_live l CROSS JOIN LATERAL jsonb_to_recordset(l.value) h({HEADER_RECORD})
), model_listing_headers AS NOT MATERIALIZED (
 SELECT {HEADER_COLUMNS} FROM library_listing_source_heads h WHERE h.graph_id=ANY($1)
 AND NOT EXISTS(SELECT 1 FROM model_dirty_items d WHERE d.graph_id=h.graph_id AND d.source_item_id=h.source_item_id)
 UNION ALL SELECT {HEADER_COLUMNS} FROM model_listing_fresh
), scope_sources AS NOT MATERIALIZED (
 SELECT h.*,h.title256 title FROM model_listing_headers h WHERE
 {scope_where}
), {threshold}model_listing_row_items AS MATERIALIZED (
 -- Fresh outcome rows only for relevant items in scope: an item out of
 -- scope joins no scope source, so its rows could never be read.
 SELECT f.graph_id,f.source_item_id FROM model_listing_fresh f WHERE {fresh_in_scope}{reach}
), model_listing_rows_live AS MATERIALIZED ({rows}),
 model_listing_fresh_outcomes AS MATERIALIZED (
 SELECT o.* FROM model_listing_rows_live l CROSS JOIN LATERAL jsonb_to_recordset(l.value) o({OUTCOME_RECORD})
), scope_atoms AS NOT MATERIALIZED (
 SELECT o.graph_id,o.semantic_atom_id,o.source_version_id,o.source_item_id,o.occurred_at,o.captured_at,o.source_version_is_current
 FROM library_listing_outcomes o JOIN scope_sources s ON s.graph_id=o.graph_id AND s.source_item_id=o.source_item_id
 WHERE o.graph_id=ANY($1) {mode} AND NOT EXISTS(SELECT 1 FROM model_dirty_items d WHERE d.graph_id=o.graph_id AND d.source_item_id=o.source_item_id)
 UNION ALL SELECT o.graph_id,o.semantic_atom_id,o.source_version_id,o.source_item_id,o.occurred_at,o.captured_at,o.source_version_is_current
 FROM model_listing_fresh_outcomes o JOIN scope_sources s ON s.graph_id=o.graph_id AND s.source_item_id=o.source_item_id WHERE true {mode}
)"#)
}

const OUTCOME_CURSOR: &str = r#"($11::uuid IS NULL
 OR ($16::timestamptz IS NOT NULL AND (o.occurred_at<$16 OR (o.occurred_at=$16 AND (o.captured_at,o.semantic_atom_id)<($10,$11)) OR o.occurred_at IS NULL))
 OR ($16::timestamptz IS NULL AND o.occurred_at IS NULL AND (o.captured_at,o.semantic_atom_id)<($10,$11)))"#;

/// The ranked shape's clean outcome page: each graph's stored rows outside
/// the overlay, after the cursor, in page order, limited per graph and then
/// globally. Each graph's prefix contains every clean row of that graph that
/// can enter the global page.
fn clean_outcome_page(outcome: &str, mode: &str) -> String {
    format!(r#"model_clean_outcome_page AS MATERIALIZED (
 SELECT ranked.* FROM (
 SELECT ranked_local.* FROM model_graphs g CROSS JOIN LATERAL (
 SELECT {outcome} FROM library_listing_outcomes o WHERE o.graph_id=g.id {mode}
 AND NOT EXISTS(SELECT 1 FROM model_dirty_items d WHERE d.graph_id=o.graph_id AND d.source_item_id=o.source_item_id)
 AND {OUTCOME_CURSOR} ORDER BY o.occurred_at DESC NULLS LAST,o.captured_at DESC,o.semantic_atom_id DESC LIMIT $15) ranked_local
 ORDER BY occurred_at DESC NULLS LAST,captured_at DESC,semantic_atom_id DESC LIMIT $15) ranked
)"#)
}

/// Only these page keys evaluate the canonical jobs expression, under the caller
/// role in the same snapshot. No jobs-derived value is shared in the cache.
pub(super) fn page_details() -> String {
    let status = super::super::EXTRACTION_STATUS_SQL.replace("__VERSION__", "v");
    // The same canonical status over a published head: its selected version,
    // raw status and next start are what the version row would supply.
    let head_status = super::super::EXTRACTION_STATUS_SQL
        .replace("__VERSION__.extraction_status", "h.raw_extraction_status")
        .replace("__VERSION__.version_id", "h.selected_version_id")
        .replace("__VERSION__.next_start_char", "h.raw_next_start_char")
        .replace("__VERSION__", "h");
    format!(r#"listing_page_keys AS MATERIALIZED (
 SELECT graph_id,source_item_id FROM source_page
), listing_page_source_details AS MATERIALIZED (
 SELECT s.*,raw.updated_at,v.origin version_origin,v.source_metadata,v.representation,v.occurred_at,v.is_legacy_baseline,
 {status} effective_extraction_status,
 s.raw_extraction_reason extraction_error,s.raw_extraction_reason current_extraction_error,s.captured_at current_captured_at
 FROM listing_page_keys k CROSS JOIN LATERAL (SELECT s.* FROM scope_sources s
 WHERE s.graph_id=k.graph_id AND s.source_item_id=k.source_item_id OFFSET 0) s
 JOIN source_items raw ON raw.graph_id=s.graph_id AND raw.source_item_id=s.source_item_id AND raw.access_state='permitted'
 LEFT JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id AND v.version_id=s.selected_version_id
), listing_page_sources AS MATERIALIZED (
 SELECT d.*,d.effective_extraction_status selected_extraction_status,d.effective_extraction_status current_extraction_status
 FROM listing_page_source_details d
), source_page_details AS MATERIALIZED (
 SELECT s.* FROM source_page p JOIN listing_page_sources s ON s.graph_id=p.graph_id AND s.source_item_id=p.source_item_id
), permitted_items AS MATERIALIZED (
 SELECT r.graph_id,r.source_item_id FROM source_items r WHERE r.graph_id=ANY($1) AND r.access_state='permitted'
), node_item_sources AS MATERIALIZED (
 -- The page nodes' items take their capture facts from the published heads
 -- in one hashed pass, not a source_versions probe per item (2,376 probes on
 -- the measured org tenant); access is rechecked against source_items the
 -- same way. Full version details stay for the source-page rows above.
 SELECT h.graph_id,h.source_item_id,h.unknown_independence,h.current_version_id,h.captured_at current_captured_at,
 {head_status} current_extraction_status
 FROM (SELECT DISTINCT graph_id,source_item_id FROM page_model_items) k
 JOIN scope_sources h ON h.graph_id=k.graph_id AND h.source_item_id=k.source_item_id
 JOIN permitted_items raw ON raw.graph_id=h.graph_id AND raw.source_item_id=h.source_item_id
), "#)
}

pub(super) fn totals(scope: &LibraryScope) -> String {
    let count = if super::super::reads_served_versions_only(scope) { "served_count" } else { "retained_count" };
    format!(r#"model_listing_totals AS MATERIALIZED (
 -- Sources, outcomes and unknown origins aggregate over the scope's heads
 -- (a dirty item is already its fresh row). The node count is the maintained
 -- per-graph count with each dirty node's stored contribution swapped for its
 -- live recomputation: the stored row is exactly what the count included.
 SELECT count(*)::bigint source_count,COALESCE(sum(h.{count}),0)::bigint outcome_count,
 ((SELECT COALESCE(sum(g.node_count),0) FROM model_graphs g)
  -(SELECT count(*) FROM library_node_evidence n JOIN model_dirty d USING(graph_id,node_id) WHERE n.admitted_unfiltered)
  +(SELECT count(*) FROM model_live l CROSS JOIN LATERAL jsonb_to_recordset(l.value->'nodes') n(admitted_unfiltered boolean) WHERE n.admitted_unfiltered))::bigint node_count,
 count(*) FILTER(WHERE h.origin IS NULL)::bigint unknown_origin_count
 FROM scope_sources h
), "#)
}

/// Facets are one GROUP BY over the scope's heads, clean or dirty alike: a
/// dirty item is already its fresh row in `scope_sources`. Keys keep full
/// JSONB equality; a container's label is the canonical maximum over its
/// sources' labels.
pub(super) fn facets() -> String {
    r#"model_facet_counts AS MATERIALIZED (
 SELECT dimension,key,count(*)::bigint source_count,max(label) label FROM (
 SELECT 'service'::text dimension,to_jsonb(h.origin->>'service_id') key,NULL::text label FROM scope_sources h WHERE h.origin->>'service_id' IS NOT NULL
 UNION ALL SELECT 'content_kind',to_jsonb(h.discovery_content_kind),NULL::text FROM scope_sources h WHERE h.discovery_content_kind IS NOT NULL
 UNION ALL SELECT 'connection',jsonb_build_object('service_id',h.origin->'service_id','connection',h.origin->'connection'),NULL::text FROM scope_sources h WHERE jsonb_typeof(h.origin->'connection')='object'
 UNION ALL SELECT 'container',jsonb_build_object('service_id',h.origin->'service_id','container',(h.origin->'container')-'label'),h.origin->'container'->>'label' FROM scope_sources h WHERE jsonb_typeof(h.origin->'container')='object'
 ) f GROUP BY dimension,key
), service_facets AS (SELECT key#>>'{}' value,source_count FROM model_facet_counts WHERE dimension='service' ORDER BY 1),
 kind_facets AS (SELECT key#>>'{}' value,source_count FROM model_facet_counts WHERE dimension='content_kind' ORDER BY 1),
 connection_facets AS (SELECT key value,source_count FROM model_facet_counts WHERE dimension='connection' ORDER BY 1),
 container_facets AS (SELECT key value,label,source_count FROM model_facet_counts WHERE dimension='container' ORDER BY 1)
"#.to_owned()
}

/// Per-graph indexed prefixes contain every row that can enter the global page.
/// The complete dirty replacement joins that merge before the global limit.
/// This does not reinterpret an output LIMIT as a physical page/IO guarantee.
/// Outcomes merge the clean page (`model_clean_outcome_page`) with the fresh
/// rows that can reach it (`inputs`: the θ bound).
pub(super) fn rank_pages(mut tail: String, scope: &LibraryScope) -> String {
    let header = HEADER_COLUMNS.split(',').map(|c| format!("h.{c}")).collect::<Vec<_>>().join(",");
    let outcome = OUTCOME_COLUMNS.split(',').map(|c| format!("o.{c}")).collect::<Vec<_>>().join(",");
    let source_cursor = r#"($9::uuid IS NULL
 OR ($8::timestamptz IS NOT NULL AND (h.captured_at<$8 OR (h.captured_at=$8 AND h.source_item_id<$9) OR h.captured_at IS NULL))
 OR ($8::timestamptz IS NULL AND h.captured_at IS NULL AND h.source_item_id<$9))"#;
    tail = super::replace_cte(tail, "source_page", "atom_page_keys", &format!(r#"AS MATERIALIZED (
 SELECT ranked.*,ranked.title256 title FROM (
 SELECT ranked_local.* FROM model_graphs g CROSS JOIN LATERAL (
 SELECT {header} FROM library_listing_source_heads h WHERE h.graph_id=g.id
 AND NOT EXISTS(SELECT 1 FROM model_dirty_items d WHERE d.graph_id=h.graph_id AND d.source_item_id=h.source_item_id)
 AND {source_cursor} ORDER BY h.captured_at DESC NULLS LAST,h.source_item_id DESC LIMIT $15) ranked_local
 UNION ALL SELECT {header} FROM model_listing_fresh h WHERE {source_cursor}
 ORDER BY captured_at DESC NULLS LAST,source_item_id DESC LIMIT $15) ranked
)"#));
    let mode = if super::super::reads_served_versions_only(scope) { "AND o.source_version_is_current" } else { "" };
    super::replace_cte(tail, "atom_page_keys", "atom_page", &format!(r#"AS MATERIALIZED (
 SELECT ranked.* FROM (
 SELECT {outcome} FROM model_clean_outcome_page o
 UNION ALL SELECT {outcome} FROM model_listing_fresh_outcomes o WHERE true {mode} AND {OUTCOME_CURSOR}
 ORDER BY occurred_at DESC NULLS LAST,captured_at DESC,semantic_atom_id DESC LIMIT $15) ranked
)"#))
}
