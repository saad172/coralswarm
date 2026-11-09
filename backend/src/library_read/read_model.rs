//! Definition-3 listing program: clean admission and maintained facts share one
//! statement; only successful NeedsDirty dispatches the exact bounded overlay.
//! Both Model bodies and unchanged canonical Live share one read-only RR wall.
use serde::{Deserialize, Serialize};
use super::LibraryScope;
mod listing;

/// The Model path is the default. `LIBRARY_READ_MODEL` is only a kill
/// switch: `off` serves Live for every request; anything else serves the
/// Model. No value refuses boot: a bad kill switch must not take prod down.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode { Off, #[default] On }
impl Mode {
    /// The mode for an env value, plus the warning a value outside
    /// `off`/`on` earns (`shadow` is the retired proof stage).
    pub fn parse(value: Option<&str>) -> (Self, Option<&'static str>) {
        match value.map(str::trim) {
            None | Some("") => (Self::On, None),
            Some(v) if v.eq_ignore_ascii_case("off") => (Self::Off, None),
            Some(v) if v.eq_ignore_ascii_case("on") => (Self::On, None),
            Some(v) if v.eq_ignore_ascii_case("shadow") => (Self::On, Some("LIBRARY_READ_MODEL=shadow is retired; serving the Model path")),
            Some(_) => (Self::On, Some("LIBRARY_READ_MODEL is not off or on; serving the Model path")),
        }
    }
}

static MODE: std::sync::OnceLock<Mode> = std::sync::OnceLock::new();
/// The process's mode, read from the environment once. Boot calls this to
/// log the decision; a request that reaches it first gets the same answer.
pub fn mode() -> Mode {
    *MODE.get_or_init(|| {
        let value = std::env::var("LIBRARY_READ_MODEL").ok();
        let (mode, warning) = Mode::parse(value.as_deref());
        match warning {
            Some(warning) => tracing::warn!(value = value.as_deref().unwrap_or_default(), "{warning}"),
            None => tracing::info!(mode = ?mode, "Library read model mode"),
        }
        mode
    })
}

/// Hash of every generated SQL variant and the program contract. It is
/// informational: `library_read_model_diff --describe`/`--export-sql` print
/// it and `scripts/check_verdict_once.py` recomputes it from the export.
/// Nothing at boot compares it.
pub fn definition_sha256() -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for current in [false, true] { for filtered in [false, true] { for (_, query) in DEFINITION_QUERY_VARIANTS {
        let mut scope = LibraryScope::exact_node(uuid::Uuid::nil(), uuid::Uuid::nil());
        scope.node_id = None;
        scope.outcome_versions = Some(if current { super::OutcomeVersions::Current } else { super::OutcomeVersions::AllRetained });
        if filtered { scope.collection = Some(super::LibraryCollection::Meetings); }
        let program = definition_program(&scope, query);
        for part in [serde_json::to_string(&program["contract"]).expect("static program contract"),
            program["clean_model_attempt_sql"].as_str().expect("clean Model SQL").to_owned(),
            program["dirty_model_attempt_sql"].as_str().expect("dirty Model SQL").to_owned(),
            program["conditional_live_sql"].as_str().expect("live SQL").to_owned()] {
            hash.update(part.as_bytes());
            hash.update([0]);
        }
    }}}
    format!("{:x}", hash.finalize())
}

/// Stable export/hash order for every text-dependent statement shape. Empty
/// unfiltered reads inline rank inputs; nonempty short reads retain their fence.
/// Values are representatives only: actual query text remains a bound input.
pub const DEFINITION_QUERY_VARIANTS: [(&str, &str); 3] = [
    ("empty", ""), ("short", "a"), ("indexed", "indexed"),
];

/// Export the exact bound statement for operator EXPLAIN/benchmark receipts.
/// This does not execute SQL or interpolate account, graph or request inputs.
pub fn definition_sql(scope:&LibraryScope,query:&str)->String {sql(scope,query)}
pub fn definition_dirty_sql(scope:&LibraryScope,query:&str)->String {dirty_sql(scope,query)}

/// Test support: the dirty statement's own admission, rendered alone, with
/// the verdict and the keys it overlays (design §5.5 item 6, "assert through
/// an exported debug count"). Binds $1..$6 of the read statement.
#[cfg(feature="test-support")]
pub fn overlay_keys_sql()->String {
    format!(r#"{} SELECT jsonb_build_object('reason',(SELECT reason FROM model_admission),'hub',(SELECT hub FROM model_hub),
 'items',COALESCE((SELECT jsonb_agg(jsonb_build_array(graph_id,source_item_id) ORDER BY graph_id,source_item_id) FROM model_dirty_items),'[]'::jsonb),
 'nodes',COALESCE((SELECT jsonb_agg(jsonb_build_array(graph_id,node_id) ORDER BY graph_id,node_id) FROM model_dirty),'[]'::jsonb),
 'change_nodes',(SELECT count(*) FROM change_nodes))"#,listing::admission(false))
}

/// Versioned execution contract. Cache definition/context are independent.
/// 3: admission compares the reader's access class (tenant 0206 / control
/// 0391) instead of the role-bearing context fingerprint, and can answer
/// `access_contract_changed`.
/// 4: busy graphs (#1524 blocker 2). Admission counts distinct pending keys
/// (`crate::library_changes`) instead of raw log rows, overlays only the
/// keys relevant to the read, replaces the 60 s change age with a worker
/// heartbeat, and serves a hub's nodes stale for a bounded window (`dirty_hub`
/// past it), marked by the response's `freshness`.
pub const PROGRAM_VERSION: u32 = 4;
/// Schema of the `computed_context` both grains store and the reader
/// compares: the access class (`crate::read_access`).
pub const CONTEXT_VERSION: i32 = crate::read_access::CLASS_SCHEMA_VERSION;
/// Reader storage contract; isolated definition-2 workers cannot admit it.
pub const DEFINITION_VERSION: i32 = 3;
pub const PROGRAM_WALL_MILLIS: u64 = 12_000;
pub const OFFLINE_PROGRAM_WALL_MILLIS: u64 = 120_000;
pub const CLEANUP_WALL_MILLIS: u64 = 1_000;
pub fn program_contract() -> serde_json::Value {
    serde_json::json!({
        "version":PROGRAM_VERSION,"transaction":"read_only_repeatable_read",
        "stages":["guarded_clean_model_attempt","conditional_same_snapshot_dirty_model_attempt","conditional_unchanged_canonical_live"],
        "dirty_routing":"successful_needs_dirty_only_same_snapshot",
        "listing_definition":DEFINITION_VERSION,
        "change_budget":{"distinct_items":CHANGE_ITEM_LIMIT,"distinct_nodes":CHANGE_NODE_LIMIT,"object_keys":CHANGE_OBJECT_KEY_LIMIT,
            "object_rows":CHANGE_OBJECT_ROW_LIMIT,"object_edges":CHANGE_OBJECT_EDGE_LIMIT,"node_raw_edges":RAW_EDGE_LIMIT,"item_inputs":DIRTY_ITEM_INPUT_LIMIT,
            "overflow_counts":"bounded_prefix_lower_bounds","relevance":"old_or_new_image_in_scope",
            "worker_heartbeat_seconds":WORKER_HEARTBEAT_SECS,"backlog_alarm_seconds":CHANGE_AGE_ALARM_SECS},
        "hub":{"serve":"stale_nodes_exact_listing","stale_seconds":HUB_STALE_SECS,"covers":["dirty_hub","raw_edge_overflow"],"removals":"live"},
        "fallback":"successful_known_reason_only","errors":"never_fall_through",
        "wall_ms":PROGRAM_WALL_MILLIS,"offline_wall_ms":OFFLINE_PROGRAM_WALL_MILLIS,
        "wall_start":"before_internal_render","wall_end":"final_json_and_timeout_restore",
        "statement_timeout":"remaining_program_wall_capped_by_original_setting",
        "cleanup_wall_ms":CLEANUP_WALL_MILLIS,
        "bind_types":["uuid[]","jsonb","uuid","uuid","uuid","text","text","timestamptz","uuid","timestamptz","uuid","bigint","bigint","uuid","bigint","timestamptz"],
        "query_settings":{"statement_timeout":"12s","plan_cache_mode":"force_custom_plan","jit":"off","work_mem":"64MB","max_parallel_workers_per_gather":"0"}
    })
}
/// All three SQL stages are sealed even though Live is rendered lazily on requests.
pub fn definition_program(scope:&LibraryScope,query:&str)->serde_json::Value {
    serde_json::json!({"contract":program_contract(),"clean_model_attempt_sql":sql(scope,query),
        "dirty_model_attempt_sql":dirty_sql(scope,query),
        "conditional_live_sql":super::search_sql_with_query(scope,query)})
}

// Busy-graph budgets (#1524 blocker 2, design §3.3 step 8). Every one counts
// DISTINCT keys or physical inputs, never change-log rows; exceeding one is
// an explicit, exact Live fallback, except a hub (see HUB_STALE_SECS). The
// values are the design's starting points, to be tuned from the busy-graph
// measurement (overlay at most about 300 ms warm).
/// Distinct pending listing items (design K_ITEMS): `dirty_item_overflow`.
pub const CHANGE_ITEM_LIMIT: usize = 512;
/// Distinct dirty nodes (design K_NODES). Over it is a hub: `dirty_hub`.
pub const CHANGE_NODE_LIMIT: usize = 256;
/// Distinct node-log object keys (item, atom, semantic atom), and the log
/// rows walked to find them: `change_overflow`.
pub const CHANGE_OBJECT_KEY_LIMIT: usize = 512;
pub const CHANGE_OBJECT_ROW_LIMIT: usize = 4096;
/// Physical inverse links of those object keys, before any is resolved:
/// `raw_edge_overflow`.
pub const CHANGE_OBJECT_EDGE_LIMIT: usize = 16_384;
/// Physical links of the dirty nodes the overlay recomputes. Over it (one
/// big node is enough) the nodes are served stale like a hub, then
/// `raw_edge_overflow` past HUB_STALE_SECS.
pub const RAW_EDGE_LIMIT: usize = 4096;
/// Versions, current outcomes and stored outcomes of the relevant dirty
/// items, each: `dirty_item_input_overflow`.
pub const DIRTY_ITEM_INPUT_LIMIT: usize = 16_384;
/// Rows probed at the head of each change log per graph for the pending
/// test and the oldest change's age.
pub const PENDING_PROBE_ROWS: usize = 128;
/// Worker heartbeat: a graph whose oldest pending change has waited this
/// long with no refresh of the graph committed since is `change_stale`.
pub const WORKER_HEARTBEAT_SECS: i64 = 300;
/// A pending change older than this is logged as an alarm
/// (`library_change_backlog_alarm`). Not a correctness rule.
pub const CHANGE_AGE_ALARM_SECS: i64 = 900;
/// A hub's (or big node's) nodes are served stale for at most this long
/// after the oldest pending node-grain change, then the read goes exact Live
/// (`dirty_hub`, or `raw_edge_overflow` for a big node).
/// Founder decision 2026-10-28 (#1524, design §3.5).
pub const HUB_STALE_SECS: i64 = 30;

/// The change-set budgets the reader admits on.
pub fn change_budgets() -> crate::library_changes::Budgets {
    crate::library_changes::Budgets {
        items: CHANGE_ITEM_LIMIT,
        nodes: CHANGE_NODE_LIMIT,
        object_keys: CHANGE_OBJECT_KEY_LIMIT,
        object_rows: CHANGE_OBJECT_ROW_LIMIT,
        object_edges: CHANGE_OBJECT_EDGE_LIMIT,
    }
}


fn cte_start(sql: &str, name: &str) -> usize {
    sql.match_indices(&format!("{name} AS")).map(|(i,_)|i).find(|&i| {
        i==0 || !(sql.as_bytes()[i-1].is_ascii_alphanumeric() || sql.as_bytes()[i-1]==b'_')
    }).expect("canonical CTE exists")
}
fn replace_cte(sql: String, name: &str, next: &str, body: &str) -> String {
    let start=cte_start(&sql,name);
    let end=start+cte_start(&sql[start..],next);
    format!("{}{} {}, {}", &sql[..start],name,body,&sql[end..])
}

/// Source admission and served-version semantics stay canonical. The candidate
/// reuses its raw outcome keys for exact graph/version existence membership;
/// counts carry base keys and only selected page keys fetch payload.
fn candidate_sql(scope: &LibraryScope,query:&str,clean:bool) -> String {
    let rank=query.is_empty() && scope.origin_filter()==serde_json::json!({}) && scope.collection_key().is_none();
    let sources=listing::inputs(clean,scope,rank);
    let evidence=if clean {
        "SELECT '{\"nodes\":[],\"items\":[]}'::jsonb value".to_owned()
    } else {
        let evidence=super::evidence::node_evidence_ctes("SELECT graph_id,node_id FROM model_dirty");
        format!("{evidence} SELECT jsonb_build_object('nodes',COALESCE((SELECT jsonb_agg(to_jsonb(n)) FROM node_evidence n),'[]'::jsonb),'items',COALESCE((SELECT jsonb_agg(to_jsonb(i)) FROM node_item_evidence i),'[]'::jsonb)) value")
    };
    let mut prefix=format!(r#"WITH scope_node_items AS (SELECT NULL::uuid graph_id,NULL::uuid source_item_id,NULL::uuid version_id WHERE false),
 {sources}, model_live AS MATERIALIZED (
 {evidence}
), model_nodes AS MATERIALIZED (
 SELECT n.* FROM library_node_evidence n WHERE n.graph_id=ANY($1) AND NOT EXISTS(SELECT 1 FROM model_dirty d WHERE d.graph_id=n.graph_id AND d.node_id=n.node_id)
 UNION ALL
 SELECT n.*, {version} computed_version,now() computed_at FROM model_live l CROSS JOIN LATERAL jsonb_to_recordset(l.value->'nodes') n(
 graph_id uuid,node_id uuid,source_count bigint,unknown_independence_record_count bigint,atom_count_retained bigint,atom_count_served bigint,
 unmapped_legacy_count bigint,admitted_unfiltered boolean,has_source_link boolean,title256 text,title_visible boolean,project_mention boolean,
 work_eligible boolean,stats_admitted boolean)
), model_items AS MATERIALIZED (
 SELECT i.* FROM library_node_item_evidence i WHERE i.graph_id=ANY($1) AND NOT EXISTS(SELECT 1 FROM model_dirty d WHERE d.graph_id=i.graph_id AND d.node_id=i.node_id)
 UNION ALL
 SELECT i.* FROM model_live l CROSS JOIN LATERAL jsonb_to_recordset(l.value->'items') i(
 graph_id uuid,node_id uuid,source_item_id uuid,semantic_link boolean,legacy_link boolean,legacy_independent_version boolean,
 legacy_synthetic boolean,atom_count_retained bigint,atom_count_served bigint,project_mention boolean,
 semantic_independent_version boolean,semantic_synthetic boolean,semantic_latest_captured_at timestamptz,
 entity_type_has_match boolean,entity_type_value text,entity_type_captured_at timestamptz,entity_type_semantic_atom_id uuid,entity_type_ambiguous boolean)
), model_filtered AS MATERIALIZED (
 SELECT i.graph_id,i.node_id,count(*) FILTER(WHERE NOT s.unknown_independence)::bigint source_count,
 count(*) FILTER(WHERE s.unknown_independence)::bigint unknown_independence_record_count,
 sum(i.atom_count_retained)::bigint atom_count_retained,sum(i.atom_count_served)::bigint atom_count_served,bool_or(i.project_mention) project_mention
 FROM model_items i JOIN scope_sources s ON s.graph_id=i.graph_id AND s.source_item_id=i.source_item_id GROUP BY 1,2
), scope_nodes AS MATERIALIZED (
 SELECT n.graph_id,n.node_id,
 CASE WHEN {unfiltered} THEN n.source_count ELSE f.source_count END source_count,
 CASE WHEN {unfiltered} THEN n.unknown_independence_record_count ELSE f.unknown_independence_record_count END unknown_independence_record_count,
 CASE WHEN {unfiltered} THEN n.{atom_count} ELSE f.{atom_count} END atom_count,
 CASE WHEN {unfiltered} THEN n.atom_count_served ELSE f.atom_count_served END current_atom_count,
 n.unmapped_legacy_count,n.title256,n.title_visible,n.work_eligible,
 CASE WHEN {unfiltered} THEN n.project_mention ELSE f.project_mention END project_mention
 FROM model_nodes n LEFT JOIN model_filtered f ON f.graph_id=n.graph_id AND f.node_id=n.node_id
 WHERE CASE WHEN {unfiltered} THEN n.admitted_unfiltered ELSE f.node_id IS NOT NULL END
)"#,
 version=super::evidence::READ_MODEL_VERSION,unfiltered=super::evidence::SCOPE_BINDS.unfiltered,
 atom_count=if super::reads_served_versions_only(scope){"atom_count_served"}else{"atom_count_retained"});
    if scope.origin_filter() == serde_json::json!({}) && scope.collection_key().is_none() {
        let start=prefix.find("scope_nodes AS MATERIALIZED (").expect("model nodes CTE");
        let atom_count=if super::reads_served_versions_only(scope) {"atom_count_served"} else {"atom_count_retained"};
        prefix=format!("{}scope_nodes AS MATERIALIZED (SELECT n.graph_id,n.node_id,n.source_count,n.unknown_independence_record_count,n.{atom_count} atom_count,n.atom_count_served current_atom_count,n.unmapped_legacy_count,n.title256,n.title_visible,n.work_eligible,n.project_mention FROM model_nodes n WHERE n.admitted_unfiltered)",&prefix[..start]);
    }
    // Reuse the live JSON/projections, replacing only evidence consumers. The
    // byte-identical live SQL remains a separate branch and independent oracle.
    let mut tail=super::SEARCH_SQL.to_owned();
    // Counts and text matches retain keys, never the full semantic row. Short
    // queries still read admitted statement/passage text to decide membership.
    if query.is_empty() {
        tail=replace_cte(tail,"matched_atoms","matched_sources",r#"AS MATERIALIZED (
 SELECT a.* FROM scope_atoms a
)"#);
    }else if query.chars().count()<3 || scope.source_item_id.is_some() || scope.source_version_id.is_some() || scope.node_id.is_some() {
        tail=replace_cte(tail,"matched_atoms","matched_sources",r#"AS MATERIALIZED (
 SELECT a.* FROM scope_atoms a WHERE $7::text IS NULL
 OR EXISTS(SELECT 1 FROM semantic_atoms text_atom WHERE text_atom.graph_id=a.graph_id AND text_atom.semantic_atom_id=a.semantic_atom_id
  AND text_atom.source_version_id=a.source_version_id AND (text_atom.statement ILIKE $7
  OR EXISTS(SELECT 1 FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.quote ILIKE $7)) OFFSET 0)
 OR EXISTS(SELECT 1 FROM scope_sources s WHERE s.source_item_id=a.source_item_id AND s.title ILIKE $7)
)"#);
    }
    // The full NULL-aware cursor/order/LIMIT runs before keyed payload reads.
    let page_start=cte_start(&tail,"atom_page");
    let page_end=page_start+cte_start(&tail[page_start..],"atom_page_nodes");
    let key_page=tail[page_start..page_end].replacen("atom_page AS (","atom_page_keys AS MATERIALIZED (",1);
    let payload_page=r#"atom_page AS MATERIALIZED (
 SELECT k.graph_id,k.semantic_atom_id,k.source_version_id,k.source_item_id,k.occurred_at,k.captured_at,k.source_version_is_current,
  payload.kind,payload.statement,v.origin,v.representation,v.source_metadata,v.is_legacy_baseline
 FROM atom_page_keys k
 CROSS JOIN LATERAL (SELECT a.kind,a.statement FROM semantic_atoms a
  WHERE a.graph_id=k.graph_id AND a.semantic_atom_id=k.semantic_atom_id AND a.source_version_id=k.source_version_id OFFSET 0) payload
 CROSS JOIN LATERAL (SELECT v.origin,v.representation,v.source_metadata,v.is_legacy_baseline FROM source_versions v
  WHERE v.graph_id=k.graph_id AND v.version_id=k.source_version_id AND v.source_item_id=k.source_item_id OFFSET 0) v
 ORDER BY k.occurred_at DESC NULLS LAST,k.captured_at DESC,k.semantic_atom_id DESC
), "#;
    tail.replace_range(page_start..page_end,&format!("{key_page}{payload_page}"));
    tail=replace_cte(tail,"title_matched_nodes","matched_nodes",r#"AS MATERIALIZED (
 SELECT n.graph_id,n.node_id FROM scope_nodes n WHERE $7::text IS NOT NULL AND n.title256 ILIKE $7
)"#);
    tail=replace_cte(tail,"matched_nodes","source_page",r#"AS MATERIALIZED (
 SELECT n.* FROM scope_nodes n WHERE $7::text IS NULL OR (n.title256 ILIKE $7 AND n.title_visible)
)"#);
    // Project-mention membership comes from the item grain after the source
    // filter; an unfiltered project mention must not leak into filtered work.
    let begin=tail.find("project_mentions AS MATERIALIZED (").expect("project CTE");
    let end=tail.find("work_page AS (").expect("work CTE");
    tail=format!("{}project_mention_nodes AS MATERIALIZED (SELECT graph_id,node_id FROM scope_nodes WHERE project_mention), {}",&tail[..begin],&tail[end..]);
    tail=tail.replace("AND library_node_title(n.graph_id,n.node_id) IS NOT NULL\n ORDER BY n.source_count", "AND n.title_visible\n ORDER BY n.source_count");
    if scope.origin_filter()==serde_json::json!({}) && scope.collection_key().is_none() {
        // The exact unfiltered work predicate, including title visibility,
        // is already in the merged grain. Rank six keys before any raw body
        // read; the live query's join otherwise hashes every tenant node.
        tail=replace_cte(tail,"work_page","page_nodes",r#"AS (
 SELECT n.*,__NODE_COLUMNS__ FROM (SELECT n.* FROM matched_nodes n WHERE n.source_count>0 AND n.work_eligible
 ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC LIMIT 6) n
 CROSS JOIN LATERAL (SELECT raw.* FROM nodes raw WHERE raw.graph_id=n.graph_id AND raw.node_id=n.node_id OFFSET 0) raw
)"#);
        if query.is_empty() {
            let atom_count=if super::reads_served_versions_only(scope){"atom_count_served"}else{"atom_count_retained"};
            let projection=format!("n.graph_id,n.node_id,n.source_count,n.unknown_independence_record_count,n.{atom_count} atom_count,n.atom_count_served current_atom_count,n.unmapped_legacy_count,n.title256,n.title_visible,n.work_eligible,n.project_mention");
            // Every global clean top-six row occurs in its graph's clean
            // top six AFTER all eligibility and dirty exclusions. Merge all
            // exact dirty replacements before global rank; the raw lookup
            // stays after that limit, including its existing no-fill behavior.
            tail=replace_cte(tail,"work_page","page_nodes",&format!(r#"AS (
 SELECT n.*,__NODE_COLUMNS__ FROM (
 SELECT ranked.* FROM (SELECT DISTINCT id FROM model_graphs) g CROSS JOIN LATERAL (
  SELECT {projection} FROM library_node_evidence n WHERE n.graph_id=g.id AND n.admitted_unfiltered AND n.work_eligible AND n.source_count>0
  AND NOT EXISTS(SELECT 1 FROM model_dirty d WHERE d.graph_id=n.graph_id AND d.node_id=n.node_id)
  ORDER BY n.source_count DESC,n.{atom_count} DESC,n.node_id DESC LIMIT 6
 ) ranked
 UNION ALL
 SELECT {projection} FROM model_live l CROSS JOIN LATERAL jsonb_to_recordset(l.value->'nodes') n(
 graph_id uuid,node_id uuid,source_count bigint,unknown_independence_record_count bigint,atom_count_retained bigint,atom_count_served bigint,
 unmapped_legacy_count bigint,admitted_unfiltered boolean,has_source_link boolean,title256 text,title_visible boolean,project_mention boolean,
 work_eligible boolean,stats_admitted boolean)
 WHERE n.admitted_unfiltered AND n.work_eligible AND n.source_count>0
 ORDER BY source_count DESC,atom_count DESC,node_id DESC LIMIT 6
 ) n CROSS JOIN LATERAL (SELECT raw.* FROM nodes raw WHERE raw.graph_id=n.graph_id AND raw.node_id=n.node_id OFFSET 0) raw
)"#));
        }
    }
    // Materialize the per-outcome lateral work itself: both page consumers
    // reuse these links without repeating the underlying inverse probes.
    let prefix_at=cte_start(&tail,"atom_page_nodes");
    tail.insert_str(prefix_at,r#"atom_page_link_prefix AS MATERIALIZED (
 SELECT a.graph_id,a.semantic_atom_id,l.node_id FROM atom_page a
 CROSS JOIN LATERAL (SELECT l.node_id FROM node_semantic_atoms l
 WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id
 ORDER BY l.node_id LIMIT 21) l
), "#);
    tail=replace_cte(tail,"atom_page_nodes","atom_page_links",r#"AS MATERIALIZED (
 -- Each link's actual page outcome already proves library_node_permitted's
 -- semantic-source EXISTS: same graph, version and permitted scope source.
 -- The first21 ordered links suffice for the first20 and exact >20 flag.
 -- The composite graph/node FK and graph-bound node policies preserve every
 -- incoming permitted outcome witness, including nodes with a NULL title.
 SELECT d.graph_id,d.node_id,n.type FROM (SELECT DISTINCT graph_id,node_id FROM atom_page_link_prefix) d
 CROSS JOIN LATERAL (SELECT n.type FROM nodes n WHERE n.node_id=d.node_id AND n.graph_id=d.graph_id OFFSET 0) n
)"#);
    tail=replace_cte(tail,"atom_page_links","atom_page_titles",r#"AS MATERIALIZED (
 SELECT l.graph_id,l.semantic_atom_id,l.node_id,row_number() OVER(PARTITION BY l.graph_id,l.semantic_atom_id ORDER BY l.node_id) shown
 FROM atom_page_link_prefix l
 JOIN atom_page_nodes p ON p.graph_id=l.graph_id AND p.node_id=l.node_id
)"#);
    // A hub serves stored node rows; the outcome page's node titles are read
    // through the canonical title function instead (Live's own expression),
    // so outcomes stay exact while nodes are stale.
    tail=replace_cte(tail,"atom_page_titles","node_page",r#"AS MATERIALIZED (
 SELECT d.graph_id,d.node_id,CASE WHEN (SELECT hub FROM model_hub) THEN library_node_title(d.graph_id,d.node_id)
 WHEN EXISTS(SELECT 1 FROM model_dirty k WHERE k.graph_id=d.graph_id AND k.node_id=d.node_id)
 THEN (SELECT CASE WHEN n.title_visible THEN n.title256 END FROM model_live m
  CROSS JOIN LATERAL jsonb_to_recordset(m.value->'nodes') n(graph_id uuid,node_id uuid,title256 text,title_visible boolean)
  WHERE n.graph_id=d.graph_id AND n.node_id=d.node_id)
 ELSE (SELECT CASE WHEN n.title_visible THEN n.title256 END FROM library_node_evidence n WHERE n.graph_id=d.graph_id AND n.node_id=d.node_id) END title
 FROM (SELECT DISTINCT graph_id,node_id FROM atom_page_links WHERE shown<=20) d
)"#);
    if query.is_empty() && scope.origin_filter()==serde_json::json!({}) && scope.collection_key().is_none() {
        let atom_count=if super::reads_served_versions_only(scope){"atom_count_served"}else{"atom_count_retained"};
        // Each graph's clean topK contains every clean row that can enter the
        // global topK. Exclude dirty pairs and apply the cursor BEFORE that
        // local limit, then merge all exact dirty replacements before ranking.
        // DISTINCT graph IDs preserves ANY semantics for duplicated inputs.
        let projection=format!("n.graph_id,n.node_id,n.source_count,n.unknown_independence_record_count,n.{atom_count} atom_count,n.atom_count_served current_atom_count,n.unmapped_legacy_count,n.title256,n.title_visible,n.work_eligible,n.project_mention");
        tail=replace_cte(tail,"node_page","project_mention_nodes",&format!(r#"AS (
 SELECT n.*,__NODE_COLUMNS__ FROM (
 SELECT ranked.* FROM (SELECT DISTINCT id FROM model_graphs) g CROSS JOIN LATERAL (
  SELECT {projection} FROM library_node_evidence n WHERE n.graph_id=g.id AND n.admitted_unfiltered
  AND NOT EXISTS(SELECT 1 FROM model_dirty d WHERE d.graph_id=n.graph_id AND d.node_id=n.node_id)
  AND ($14::uuid IS NULL OR (n.source_count,n.{atom_count},n.node_id)<($12,$13,$14))
  ORDER BY n.source_count DESC,n.{atom_count} DESC,n.node_id DESC LIMIT $15
 ) ranked
 UNION ALL
 SELECT {projection} FROM model_live l CROSS JOIN LATERAL jsonb_to_recordset(l.value->'nodes') n(
 graph_id uuid,node_id uuid,source_count bigint,unknown_independence_record_count bigint,atom_count_retained bigint,atom_count_served bigint,
 unmapped_legacy_count bigint,admitted_unfiltered boolean,has_source_link boolean,title256 text,title_visible boolean,project_mention boolean,
 work_eligible boolean,stats_admitted boolean)
 WHERE n.admitted_unfiltered AND ($14::uuid IS NULL OR (n.source_count,n.{atom_count},n.node_id)<($12,$13,$14))
 ORDER BY source_count DESC,atom_count DESC,node_id DESC LIMIT $15
 ) n CROSS JOIN LATERAL (SELECT raw.* FROM nodes raw WHERE raw.node_id=n.node_id AND raw.graph_id=n.graph_id OFFSET 0) raw
)"#));
    } else {
        tail=replace_cte(tail,"node_page","project_mention_nodes",r#"AS (
 SELECT n.*,__NODE_COLUMNS__ FROM (SELECT n.* FROM matched_nodes n WHERE $14::uuid IS NULL OR (n.source_count,n.atom_count,n.node_id)<($12,$13,$14)
 ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC LIMIT $15) n
 CROSS JOIN LATERAL (SELECT raw.* FROM nodes raw WHERE raw.node_id=n.node_id AND raw.graph_id=n.graph_id OFFSET 0) raw
)"#);
    }
    tail=replace_cte(tail,"page_legacy","page_capture",r#"AS MATERIALIZED (
 SELECT x.graph_id,x.node_id,array_agg(DISTINCT x.source_item_id) FILTER(WHERE x.legacy_independent_version) independent_items,
 bool_or(x.legacy_synthetic) synthetic FROM page_model_items x
 JOIN scope_sources s ON s.graph_id=x.graph_id AND s.source_item_id=x.source_item_id WHERE x.legacy_link GROUP BY 1,2
)"#);
    tail=replace_cte(tail,"page_capture","page_entity_types",r#"AS MATERIALIZED (
 SELECT links.graph_id,links.node_id,
 count(*) FILTER(WHERE NOT s.unknown_independence) source_count,
 count(*) retained_record_count,count(*) FILTER(WHERE s.current_version_id IS NOT NULL) current_version_count,
 count(*) FILTER(WHERE s.current_version_id IS NULL) unavailable_version_count,
 count(*) FILTER(WHERE s.current_extraction_status IN('not_started','pending','running')) pending_version_count,
 count(*) FILTER(WHERE s.current_extraction_status IN('failed','blocked')) incomplete_version_count,
 count(*) FILTER(WHERE s.current_version_id IS NULL OR s.current_extraction_status IN('failed','blocked')) incomplete_or_unavailable_count,
 max(s.current_captured_at) latest_captured_at
 FROM page_model_items links JOIN node_item_sources s ON s.graph_id=links.graph_id AND s.source_item_id=links.source_item_id
 GROUP BY 1,2
)"#);
    tail=tail.replace("FROM scoped_semantic_links l JOIN retained_scope_atoms", "FROM node_semantic_atoms l JOIN retained_scope_atoms");
    let page_items=r#"page_model_items AS MATERIALIZED (
 SELECT i.* FROM page_nodes p CROSS JOIN LATERAL (SELECT i.* FROM library_node_item_evidence i
 WHERE i.graph_id=p.graph_id AND i.node_id=p.node_id AND NOT EXISTS(SELECT 1 FROM model_dirty d WHERE d.graph_id=i.graph_id AND d.node_id=i.node_id) OFFSET 0) i
 UNION ALL
 SELECT i.* FROM model_live m CROSS JOIN LATERAL jsonb_to_recordset(m.value->'items') i(
 graph_id uuid,node_id uuid,source_item_id uuid,semantic_link boolean,legacy_link boolean,legacy_independent_version boolean,
 legacy_synthetic boolean,atom_count_retained bigint,atom_count_served bigint,project_mention boolean,
 semantic_independent_version boolean,semantic_synthetic boolean,semantic_latest_captured_at timestamptz,
 entity_type_has_match boolean,entity_type_value text,entity_type_captured_at timestamptz,entity_type_semantic_atom_id uuid,entity_type_ambiguous boolean)
 WHERE EXISTS(SELECT 1 FROM page_nodes p WHERE p.graph_id=i.graph_id AND p.node_id=i.node_id)
), "#;
    let page_details=listing::page_details();
    // Under a hub the page nodes' titles are read once each through the
    // canonical title function (Live's own expression), never from the
    // stored rows: a title renamed or hidden during the window is not shown.
    let page_titles=r#"page_node_titles AS MATERIALIZED (
 SELECT p.graph_id,p.node_id,library_node_title(p.graph_id,p.node_id) title FROM page_nodes p WHERE (SELECT hub FROM model_hub)
), "#;
    tail=tail.replace("page_legacy AS MATERIALIZED (",&format!("{page_items}{page_titles}{page_details}page_legacy AS MATERIALIZED ("));
    // Both page item arms already enforce the unique (graph,node) page
    // keys. Rejoining page_nodes per item only repeats that membership.
    // Historical semantic facts are shared with the canonical worker/dirty
    // definition at node/item grain. Wider context remains unfiltered; only
    // the entity winner selects items admitted by the request's source scope.
    let page_facts=r#"page_wide_context AS MATERIALIZED (
 SELECT i.graph_id,i.node_id,sum(i.atom_count_retained)::bigint semantic_atom_count,
 max(i.semantic_latest_captured_at) latest_captured_at,COALESCE(bool_or(i.semantic_synthetic),false) synthetic,
 array_agg(i.source_item_id) FILTER(WHERE i.semantic_independent_version) independent_items
 FROM page_model_items i GROUP BY 1,2
), "#;
    tail=replace_cte(tail,"page_entity_types","page_node_context",r#"AS MATERIALIZED (
 SELECT w.* FROM page_entity_winners w JOIN page_nodes p ON p.graph_id=w.graph_id AND p.node_id=w.node_id
 WHERE NOT p.entity_type_sticky
)"#);
    // Sticky is constant for the unique page-node pair, so it commutes with
    // latest-tuple selection. Materialize winners before that small lookup.
    let entity_winners=r#"page_entity_winners AS MATERIALIZED (
 SELECT DISTINCT ON (i.graph_id,i.node_id) i.graph_id,i.node_id,i.entity_type_value entity_type,i.entity_type_ambiguous
 FROM page_model_items i JOIN scope_sources s ON s.graph_id=i.graph_id AND s.source_item_id=i.source_item_id
 WHERE i.entity_type_has_match
 ORDER BY i.graph_id,i.node_id,i.entity_type_captured_at DESC,i.entity_type_semantic_atom_id DESC
), "#;
    tail=tail.replace("page_entity_types AS MATERIALIZED (",&format!("{page_facts}{entity_winners}page_entity_types AS MATERIALIZED ("));
    let context_start=cte_start(&tail,"page_node_context");
    let lateral_start=context_start+tail[context_start..].find(" CROSS JOIN LATERAL (").expect("wider context aggregate");
    let lateral_end=lateral_start+tail[lateral_start..].find(" w\n), source_item_counts").expect("wider context boundary")+2;
    tail.replace_range(lateral_start..lateral_end," LEFT JOIN page_wide_context w ON w.graph_id=p.graph_id AND w.node_id=p.node_id");
    tail=tail.replace("p.graph_id,p.node_id,w.semantic_atom_count,w.latest_captured_at", "p.graph_id,p.node_id,COALESCE(w.semantic_atom_count,0) semantic_atom_count,w.latest_captured_at")
        .replace("w.synthetic OR COALESCE(x.synthetic,false)","COALESCE(w.synthetic,false) OR COALESCE(x.synthetic,false)");
    // Source-page legacy counts use only the displayed items' atoms, with the
    // same canonical permission function and graph-qualified chunk mappings.
    let begin=tail.find("source_item_counts AS MATERIALIZED (").expect("source counts");
    let end=tail.find("service_facets AS (").expect("facets");
    tail=format!(r#"{}source_page_chunks AS MATERIALIZED (
 SELECT s.graph_id,s.source_item_id,c.atom_id FROM source_page s JOIN source_versions v ON v.graph_id=s.graph_id AND v.source_item_id=s.source_item_id
 JOIN source_version_chunks c ON c.graph_id=v.graph_id AND c.source_version_id=v.version_id WHERE $4::uuid IS NULL OR v.version_id=$4
), source_page_permitted AS MATERIALIZED (
 SELECT g.graph_id,p.atom_id FROM (SELECT DISTINCT graph_id FROM source_page_chunks) g
 CROSS JOIN LATERAL library_permitted_atoms(g.graph_id,ARRAY(SELECT DISTINCT c.atom_id FROM source_page_chunks c WHERE c.graph_id=g.graph_id)) p
), source_item_counts AS MATERIALIZED (
 SELECT s.graph_id,s.source_item_id,s.{listing_count} atom_count,COALESCE(x.legacy_chunk_count,0) legacy_chunk_count FROM source_page s
 LEFT JOIN(SELECT c.graph_id,c.source_item_id,count(DISTINCT c.atom_id) legacy_chunk_count FROM source_page_chunks c
 JOIN source_page_permitted p ON p.graph_id=c.graph_id AND p.atom_id=c.atom_id GROUP BY 1,2) x ON x.graph_id=s.graph_id AND x.source_item_id=s.source_item_id
), {}"#,&tail[..begin],&tail[end..],listing_count=if super::reads_served_versions_only(scope){"served_count"}else{"retained_count"});
    if query.is_empty() && scope.origin_filter()==serde_json::json!({}) && scope.collection_key().is_none() {
        prefix=prefix.replace("model_nodes AS MATERIALIZED (","model_nodes AS NOT MATERIALIZED (")
            .replace("scope_nodes AS MATERIALIZED (","scope_nodes AS NOT MATERIALIZED (");
        tail=tail.replace(" matched_nodes AS MATERIALIZED ("," matched_nodes AS NOT MATERIALIZED (");
    }
    // The payload and latest-job expression are evaluated only for the
    // distinct union of source-page and wider page-item keys, under caller RLS.
    tail=tail.replace(" FROM source_page s),'[]'::jsonb)"," FROM source_page_details s),'[]'::jsonb)");
    if query.is_empty() && scope.origin_filter()==serde_json::json!({}) && scope.collection_key().is_none() {
        tail=listing::rank_pages(tail,scope);
        let facet_start=cte_start(&tail,"service_facets");
        let public_start=tail.find("\nSELECT jsonb_build_object(\n 'sources'").expect("public projection");
        tail=format!("{}{}{}{}",&tail[..facet_start],listing::totals(scope),listing::facets(),&tail[public_start..]);
        tail=tail.replace("(SELECT count(*) FROM matched_sources)","(SELECT source_count FROM model_listing_totals)")
            .replace("(SELECT count(*) FROM matched_atoms)","(SELECT outcome_count FROM model_listing_totals)")
            .replace("(SELECT count(*) FROM matched_nodes)","(SELECT node_count FROM model_listing_totals)")
            .replace("(SELECT count(*) FROM matched_sources WHERE origin IS NULL)","(SELECT unknown_origin_count FROM model_listing_totals)");
    }
    // This gate reads only cached/dirty page facts. An unspecified canonical
    // type tie must choose the actual independent live reader, before the
    // public model projection executes; never choose an arbitrary cached type.
    let public_start=tail.find("\nSELECT jsonb_build_object(\n 'sources'").expect("public search projection");
    let totals_check=if query.is_empty() && scope.origin_filter()==serde_json::json!({}) && scope.collection_key().is_none() {
        " OR EXISTS(SELECT 1 FROM model_listing_totals WHERE source_count<0 OR outcome_count<0 OR node_count<0 OR unknown_origin_count<0 OR unknown_origin_count>source_count)"
    } else {""};
    tail=format!("{}, model_late_admission AS MATERIALIZED (SELECT CASE WHEN EXISTS(SELECT 1 FROM page_entity_types WHERE entity_type_ambiguous) THEN 'entity_type_ambiguous' WHEN (SELECT count(*) FROM source_page_details)<>(SELECT count(*) FROM source_page) OR (SELECT count(*) FROM atom_page)<>(SELECT count(*) FROM atom_page_keys) OR EXISTS(SELECT 1 FROM page_nodes p JOIN library_node_evidence n USING(graph_id,node_id) WHERE NOT EXISTS(SELECT 1 FROM model_dirty d WHERE d.graph_id=p.graph_id AND d.node_id=p.node_id) AND n.computed_version IS DISTINCT FROM {node_version}) OR EXISTS(SELECT 1 FROM listing_page_sources s JOIN library_listing_source_heads h USING(graph_id,source_item_id) JOIN model_graphs g ON g.id=h.graph_id WHERE NOT EXISTS(SELECT 1 FROM model_dirty_items d WHERE d.graph_id=h.graph_id AND d.source_item_id=h.source_item_id) AND h.epoch IS DISTINCT FROM g.epoch){totals_check} THEN 'listing_incomplete' END reason) SELECT jsonb_build_object('value',CASE WHEN a.reason IS NULL THEN ({}) END,'fallback_reason',a.reason) FROM model_late_admission a",&tail[..public_start],&tail[public_start..],node_version=super::evidence::READ_MODEL_VERSION);
    prefix.push_str(&tail);
    // Node page titles under a hub come from page_node_titles.
    super::render_search_projection(prefix,scope,query)
        .replace("'title',library_node_title(n.graph_id,n.node_id),'type',n.type", "'title',CASE WHEN (SELECT hub FROM model_hub) THEN (SELECT t.title FROM page_node_titles t WHERE t.graph_id=n.graph_id AND t.node_id=n.node_id) WHEN n.title_visible THEN n.title256 END,'type',n.type")
}

/// Private attempt envelope. NeedsLive has no value; the RR program must run
/// the independent canonical reader before publishing a Live result.
/// CASE short-circuits the entire candidate before its expensive inputs run.
pub(super) fn sql(scope: &LibraryScope,query:&str) -> String { attempt_sql(scope,query,true) }
/// Explicit second statement, dispatched only after the clean body returned the
/// recognized NeedsDirty envelope in the same pinned RR transaction.
pub(super) fn dirty_sql(scope:&LibraryScope,query:&str)->String { attempt_sql(scope,query,false) }
fn attempt_sql(scope:&LibraryScope,query:&str,clean:bool)->String {
    let clean_pending=if clean {" AND NOT (SELECT dirty FROM model_clean_pending)"}else{""};
    let reason=if clean {"CASE WHEN s.reason IS NULL AND (SELECT dirty FROM model_clean_pending) THEN 'dirty' WHEN s.reason IS NULL THEN 'model' ELSE 'live' END"}else{"CASE WHEN s.reason IS NULL THEN 'model' ELSE 'live' END"};
    // Only a Model value served under a hub is stale, and only its nodes.
    let freshness=if clean {"NULL::jsonb"}else{"CASE WHEN s.reason IS NULL AND (SELECT hub FROM model_hub) THEN jsonb_build_object('state','updating','stale_since',(SELECT since FROM model_hub_since),'components',jsonb_build_array('nodes')) END"};
    format!(r#"{}, model_candidate_result AS MATERIALIZED (
 SELECT a.reason preliminary_reason,CASE WHEN a.reason IS NULL{clean_pending} THEN ({}) END result FROM model_admission a
), model_selection AS MATERIALIZED (
 SELECT COALESCE(preliminary_reason,result->>'fallback_reason') reason,result FROM model_candidate_result
) SELECT jsonb_build_object('value',CASE WHEN s.reason IS NULL THEN s.result->'value' ELSE NULL::jsonb END,
 'path',{reason},'fallback_reason',s.reason,'freshness',{freshness},
 'backlog_oldest_at',(SELECT min(p.oldest_at) FROM model_pending p)) FROM model_selection s"#,
 listing::admission(clean),candidate_sql(scope,query,clean))
}

#[cfg(test)] mod tests {
    use super::*;
    #[test] fn replacement_distinguishes_title_matched_nodes_from_matched_nodes() {
        let sql="title_matched_nodes AS (SELECT 1), matched_nodes AS (SELECT 2), source_page AS (SELECT 3)";
        let replaced=replace_cte(sql.into(),"matched_nodes","source_page","AS (SELECT 4)");
        assert_eq!(replaced,"title_matched_nodes AS (SELECT 1), matched_nodes AS (SELECT 4), source_page AS (SELECT 3)");
    }
    #[test] fn only_off_turns_the_model_off_and_nothing_refuses_boot() {
        assert_eq!(Mode::parse(None),(Mode::On,None)); assert_eq!(Mode::parse(Some("")),(Mode::On,None));
        assert_eq!(Mode::parse(Some("on")),(Mode::On,None)); assert_eq!(Mode::parse(Some("off")),(Mode::Off,None));
        assert_eq!(Mode::parse(Some(" OFF ")),(Mode::Off,None));
        let (mode,warning)=Mode::parse(Some("shadow")); assert_eq!(mode,Mode::On); assert!(warning.unwrap().contains("shadow"));
        let (mode,warning)=Mode::parse(Some("true")); assert_eq!(mode,Mode::On); assert!(warning.is_some());
    }
    #[test] fn clean_program_omits_both_recomputation_trees_but_guards_both_channels() {
        let scope=LibraryScope::exact_node(uuid::Uuid::nil(),uuid::Uuid::nil());
        let mut broad=scope.clone();broad.node_id=None;
        let clean=sql(&broad,"");let dirty=dirty_sql(&broad,"");
        assert!(clean.contains("library_listing_changes"));assert!(clean.contains("library_evidence_changes"));
        assert!(clean.contains("listing_ready IS DISTINCT FROM 3"));
        assert!(clean.contains(&format!("ready_version IS DISTINCT FROM {}",crate::library_read::evidence::READ_MODEL_VERSION)));
        assert!(clean.contains("capture_since IS DISTINCT FROM switch_since"),"clean must fail closed on the listing switch");
        assert!(clean.contains("model_clean_pending"));
        for raw in ["library_evidence_change_nodes(","library_evidence_raw_edges_safe(","listing_keys AS MATERIALIZED", "node_evidence AS"] {
            assert!(!clean.contains(raw),"clean must not plan {raw}");
        }
        assert!(dirty.contains("listing_keys AS MATERIALIZED"));assert!(dirty.contains("model_listing_old_outcomes"));
        // Hybrid: heads, outcomes and one maintained node count; totals and
        // facets aggregate over the heads, in both bodies.
        for dropped in ["library_listing_graph_totals","library_listing_facet_buckets","library_listing_facet_labels","library_listing_node_contributions","model_listing_label_probe"] {
            assert!(!clean.contains(dropped)&&!dirty.contains(dropped),"{dropped} is not maintained");
        }
        for body in [&clean,&dirty] {
            assert!(body.contains("sum(g.node_count)"));assert!(body.contains("FROM scope_sources h"));
            assert!(body.contains("node_item_sources"),"page items take capture status from the heads");
        }
        assert!(clean.contains("listing_page_keys AS MATERIALIZED"));
        assert_eq!(program_contract()["version"],4);
        // Busy graphs: clean SQL only probes the head of each log; the dirty
        // body decides on distinct keys and never counts raw log rows.
        assert!(clean.contains("model_pending AS MATERIALIZED")&&!clean.contains("change_decision"));
        assert!(dirty.contains("change_decision AS MATERIALIZED")&&dirty.contains("change_relevant_items"));
        assert!(dirty.contains("library_changes_raw_edges_safe(")&&!dirty.contains("library_evidence_raw_edges_safe("));
        for body in [&clean,&dirty] {assert!(!body.contains("model_change_budget")&&!body.contains("interval '60s'"));}
        assert!(clean.contains("access_contract_changed")&&dirty.contains("access_contract_changed"));
        assert!(clean.contains("library_evidence_class(ids.id)")&&!clean.contains("library_evidence_context("),"admission compares the access class");
        for digest in [crate::read_access::TENANT_CONTRACT_DIGEST,crate::read_access::CONTROL_CONTRACT_DIGEST] {assert!(clean.contains(digest));}
    }

    #[test] fn all_twelve_programs_bind_clean_dirty_and_live_without_new_shapes() {
        for current in [false,true] {for filtered in [false,true] {for (_,query) in DEFINITION_QUERY_VARIANTS {
            let mut scope=LibraryScope::exact_node(uuid::Uuid::nil(),uuid::Uuid::nil());scope.node_id=None;
            scope.outcome_versions=Some(if current{super::super::OutcomeVersions::Current}else{super::super::OutcomeVersions::AllRetained});
            if filtered {scope.collection=Some(super::super::LibraryCollection::Meetings);}
            let program=definition_program(&scope,query);
            let clean=program["clean_model_attempt_sql"].as_str().unwrap();
            let dirty=program["dirty_model_attempt_sql"].as_str().unwrap();
            let live=program["conditional_live_sql"].as_str().unwrap();
            assert_ne!(clean,dirty);assert_eq!(live,super::super::search_sql_with_query(&scope,query));
            assert!(!clean.contains("listing_keys AS MATERIALIZED"));assert!(dirty.contains("listing_keys AS MATERIALIZED"));
            for sql in [clean,dirty] {
                assert!(!sql.contains("library_listing_facet_labels")&&!sql.contains("model_listing_label_probe"),"facets aggregate over the heads");
                assert!(sql.contains(&format!("interval '{WORKER_HEARTBEAT_SECS}s'")),"the worker heartbeat replaces the change age");
                assert!(!sql.contains("model_change_budget"),"no raw-row budget");
                // Hybrid page detail: the jobs probe runs once over the
                // source page's version rows and once over the heads of the
                // page nodes' items (no source_versions probe per item), each
                // key set in exactly one CTE.
                assert_eq!(sql.matches("ORDER BY j.created_at DESC,j.id DESC LIMIT 1").count(),2,"jobs lookup once per source-page key and once per node-item head");
                for bind in 1..=16 {assert!(sql.contains(&format!("${bind}")),"missing bind{bind}");}
            }
        }}}
    }

}
