
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
  WHERE l.graph_id=ANY($1) AND l.node_id=$5
 UNION
 SELECT DISTINCT c.graph_id,mapped.source_item_id,mapped.version_id FROM node_atoms l
  JOIN scope_graph_chunks c ON c.atom_id=l.atom_id
  JOIN scoped_permitted_atoms p ON p.graph_id=c.graph_id AND p.atom_id=c.atom_id
  JOIN scope_graph_versions mapped ON mapped.version_id=c.source_version_id AND mapped.graph_id=c.graph_id
  WHERE l.node_id=$5
), scope_sources AS MATERIALIZED (
 SELECT s.*,CASE WHEN s.origin IS NULL THEN s.source_type ELSE s.origin->>'content_kind' END discovery_content_kind,v.version_id selected_version_id,v.origin version_origin,v.source_metadata,
 v.representation,v.occurred_at,v.captured_at,v.is_legacy_baseline,
 CASE WHEN v.extraction_status='done' THEN 'done'
 ELSE COALESCE((SELECT CASE j.status WHEN 'pending' THEN 'pending' WHEN 'running' THEN 'running'
 WHEN 'dead' THEN 'failed' ELSE v.extraction_status END FROM jobs j
 WHERE j.kind IN('ExtractMeetingAtoms','ExtractSourceAtoms')
 AND j.payload->>'source_version_id'=v.version_id::text
 AND j.payload->>'graph_id'=v.graph_id::text
 AND COALESCE(j.payload->>'start_char','0')=v.next_start_char::text
 ORDER BY j.created_at DESC,j.id DESC LIMIT 1),v.extraction_status) END selected_extraction_status,CASE WHEN current_v.extraction_status='done' THEN 'done'
 ELSE COALESCE((SELECT CASE j.status WHEN 'pending' THEN 'pending' WHEN 'running' THEN 'running'
 WHEN 'dead' THEN 'failed' ELSE current_v.extraction_status END FROM jobs j
 WHERE j.kind IN('ExtractMeetingAtoms','ExtractSourceAtoms')
 AND j.payload->>'source_version_id'=current_v.version_id::text
 AND j.payload->>'graph_id'=current_v.graph_id::text
 AND COALESCE(j.payload->>'start_char','0')=current_v.next_start_char::text
 ORDER BY j.created_at DESC,j.id DESC LIMIT 1),current_v.extraction_status) END current_extraction_status,CASE WHEN v.extraction_status='blocked' AND v.extraction_error IN ('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost') THEN v.extraction_error END extraction_error,CASE WHEN current_v.extraction_status='blocked' AND current_v.extraction_error IN ('managed_admission_required','capture_actor_unknown_managed_admission_required','managed_run_paused','execution_budget_exhausted','cloud_processing_disabled','requester_access_lost','source_removed_or_access_lost') THEN current_v.extraction_error END current_extraction_error,
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
), scope_served_versions AS MATERIALIZED (
 SELECT DISTINCT ON (c.graph_id,c.source_item_id) c.graph_id,c.source_item_id,p.version_id
 FROM scope_sources s JOIN source_versions c ON c.graph_id=s.graph_id AND c.version_id=s.current_version_id
 JOIN source_versions p ON p.graph_id=c.graph_id AND p.source_item_id=c.source_item_id AND p.content_sha256=c.content_sha256
  AND p.representation=c.representation AND p.version_id<>c.version_id
 -- OFFSET 0 keeps each existence test a per-version probe: pulled up into
 -- semi-joins, PostgreSQL 16 joined the scope's sources to the graph's
 -- outcome versions on graph_id alone first (4k by 10k rows, 40 s on the
 -- p6 facets fixture) before the keyed filters.
 WHERE (c.extraction_status<>'done' OR NOT EXISTS(SELECT 1 FROM semantic_atoms x WHERE x.graph_id=c.graph_id AND x.source_version_id=c.version_id OFFSET 0))
  AND EXISTS(SELECT 1 FROM semantic_atoms x WHERE x.graph_id=p.graph_id AND x.source_version_id=p.version_id OFFSET 0)
 ORDER BY c.graph_id,c.source_item_id,p.extraction_status='done' DESC,p.captured_at DESC,p.version_id DESC
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
 WHERE ($4::uuid IS NULL OR v.version_id=$4)
), scope_atoms AS MATERIALIZED (
 SELECT a.*,v.source_item_id,v.origin,v.occurred_at,v.captured_at,v.representation,v.source_metadata,v.is_legacy_baseline,
 true source_version_is_current,s.source_type,s.title
 FROM scope_sources s LEFT JOIN scope_served_versions served ON served.graph_id=s.graph_id AND served.source_item_id=s.source_item_id
 JOIN source_versions v ON v.graph_id=s.graph_id AND v.version_id=COALESCE(served.version_id,s.current_version_id)
 JOIN semantic_atoms a ON a.graph_id=v.graph_id AND a.source_version_id=v.version_id
 WHERE $5::uuid IS NULL OR (a.graph_id,a.semantic_atom_id) IN (SELECT l.graph_id,l.semantic_atom_id FROM node_semantic_atoms l WHERE l.node_id=$5)
), scoped_chunk_atoms AS MATERIALIZED (
 SELECT c.graph_id,c.atom_id,v.source_item_id,v.version_id,
  NOT v.is_legacy_baseline AND v.source_metadata->>'independence' IS DISTINCT FROM 'derived_copy' independent,
  v.origin->>'transport'='synthetic' OR v.source_metadata->'synthetic_demo'='true'::jsonb synthetic
 FROM scoped_permitted_atoms p JOIN scope_graph_chunks c ON c.graph_id=p.graph_id AND c.atom_id=p.atom_id
 JOIN scope_graph_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id
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
 FROM (SELECT x.graph_id,x.atom_id,x.source_item_id,bool_or($4::uuid IS NULL OR x.version_id=$4) selected,bool_or(x.independent) independent,bool_or(x.synthetic) synthetic
  FROM scoped_chunk_atoms x GROUP BY 1,2,3) x
 JOIN node_atoms l ON l.atom_id=x.atom_id AND ($5::uuid IS NULL OR l.node_id=$5)
 GROUP BY 1,2,3
), scoped_semantic_links AS MATERIALIZED (
 -- The scope's retained outcomes joined to their node links once: the
 -- source and outcome readers below both walked node_semantic_atoms for
 -- the same outcomes (0.25 s each on the 2026-10-23 org tenant).
 -- A selected node ($5) is the only node any reader of this set or the
 -- legacy set below projects or counts, so both carry its links alone.
 SELECT l.graph_id,l.node_id,a.semantic_atom_id,a.source_item_id,a.source_version_is_current FROM retained_scope_atoms a
 JOIN node_semantic_atoms l ON l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id
 WHERE ($5::uuid IS NULL OR l.node_id=$5)
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
 JOIN (SELECT DISTINCT a.graph_id,a.semantic_atom_id FROM scope_atoms a) k ON k.semantic_atom_id=l.semantic_atom_id AND k.graph_id=l.graph_id
), scoped_unmapped_legacy AS MATERIALIZED (
 SELECT p.graph_id,l.node_id,count(*) unmapped_count FROM scoped_permitted_atoms p JOIN node_atoms l ON l.atom_id=p.atom_id
 WHERE p.unmapped AND ($5::uuid IS NULL OR l.node_id=$5)
  AND (($2::jsonb='{}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL)
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
   $2::jsonb='{}'::jsonb AND $3::uuid IS NULL AND $4::uuid IS NULL AND $6::text IS NULL
  FROM scoped_unmapped_legacy
 ) evidence GROUP BY 1,2
), scope_graph_nodes AS MATERIALIZED (
 -- The scope graphs' node keys from one index-only scan. Joined per
 -- admitted node instead, the planner's one-row guess for the metrics set
 -- probed the index 66k times (0.9 s on the 2026-10-23 org tenant, where a
 -- whole-ocean scope admits every node of the graph).
 SELECT n.node_id,n.graph_id FROM nodes n WHERE n.graph_id=ANY($1) AND ($5::uuid IS NULL OR n.node_id=$5)
), scope_nodes AS MATERIALIZED (
 -- Keys and metrics only. The join proves the node exists in the scope's
 -- graph; readers that show a node's own columns fetch them for their page
 -- rows (`raw.type,raw.entity_type,raw.entity_type_sticky,substring(raw.title FOR 256) raw_title`). Carrying them here fetched every admitted
 -- node's row: 66k heap probes into a 450 MB table, 0.6 s, on the
 -- 2026-10-23 org tenant.
 SELECT support.node_id,support.graph_id,
  support.source_count,support.unknown_independence_record_count,support.atom_count,support.current_atom_count,support.unmapped_legacy_count
 FROM scope_node_metrics support JOIN scope_graph_nodes n ON n.graph_id=support.graph_id AND n.node_id=support.node_id
 WHERE support.admitted
)

, matched_sources AS MATERIALIZED (
 SELECT s.* FROM scope_sources s WHERE $7::text IS NULL OR s.title ILIKE $7 OR s.source_id ILIKE $7
 OR EXISTS(SELECT 1 FROM scope_atoms a WHERE a.source_item_id=s.source_item_id AND
  (a.statement ILIKE $7 OR EXISTS(SELECT 1 FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.quote ILIKE $7)))
), matched_atoms AS MATERIALIZED (
 SELECT a.* FROM scope_atoms a WHERE $7::text IS NULL OR a.statement ILIKE $7
 OR EXISTS(SELECT 1 FROM semantic_atom_passages p WHERE p.graph_id=a.graph_id AND p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.quote ILIKE $7)
 OR EXISTS(SELECT 1 FROM scope_sources s WHERE s.source_item_id=a.source_item_id AND s.title ILIKE $7)
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
 SELECT n.*,raw.type,raw.entity_type,raw.entity_type_sticky,substring(raw.title FOR 256) raw_title FROM (SELECT n.* FROM matched_nodes n WHERE $14::uuid IS NULL OR (n.source_count,n.atom_count,n.node_id)<($12,$13,$14)
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
 SELECT n.*,raw.type,raw.entity_type,raw.entity_type_sticky,substring(raw.title FOR 256) raw_title FROM (SELECT n.* FROM matched_nodes n WHERE n.source_count>0
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
  WHERE l.node_id=p.node_id AND l.graph_id=p.graph_id AND s.access_state='permitted') w
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
  'session',(SELECT jsonb_build_object('graph_id',s.graph_id,'provider_namespace',sv.provider_namespace,'session_id',sv.session_id) FROM library_session_versions(s.graph_id) sv WHERE sv.version_id=s.selected_version_id AND sv.session_id IS NOT NULL),
  '_id',s.source_item_id,'_at',s.captured_at
 ) ORDER BY s.captured_at DESC NULLS LAST,s.source_item_id DESC) FROM source_page s),'[]'::jsonb)),
 'outcomes',jsonb_build_object('total',(SELECT count(*) FROM matched_atoms),'items',COALESCE((SELECT jsonb_agg((jsonb_build_object(
  'graph_id',a.graph_id,'semantic_atom_id',a.semantic_atom_id,'kind',a.kind,'statement',a.statement,'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,
  'origin',a.origin,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,'occurred_at',a.occurred_at,'captured_at',a.captured_at,'source_version_is_current',a.source_version_is_current,
  'nodes',COALESCE((SELECT jsonb_agg(jsonb_build_object('node_id',l.node_id,'title',t.title,'type',p.type) ORDER BY l.node_id)
   FROM atom_page_links l JOIN atom_page_nodes p ON p.graph_id=l.graph_id AND p.node_id=l.node_id JOIN atom_page_titles t ON t.graph_id=l.graph_id AND t.node_id=l.node_id
   WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id AND l.shown<=20),'[]'::jsonb),
  'citations',COALESCE((SELECT jsonb_agg(jsonb_build_object('kind','source_passage','graph_id',a.graph_id,'source_item_id',a.source_item_id,'source_version_id',a.source_version_id,
   'semantic_atom_id',a.semantic_atom_id,'passage_id',p.passage_id,'start_char',p.start_char,'end_char',LEAST(p.end_char,p.start_char+4000),'quote',substring(p.quote FOR 4000),'quote_truncated',char_length(p.quote)>4000,
   'representation',a.representation,'source_independence',CASE WHEN a.is_legacy_baseline OR a.source_metadata->>'independence'='derived_copy' THEN 'unknown' ELSE 'captured_source_item' END,'occurred_at',a.occurred_at,'captured_at',a.captured_at) ORDER BY p.start_char,p.passage_id)
   FROM (SELECT p.* FROM semantic_atom_passages p WHERE p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.graph_id=a.graph_id ORDER BY p.start_char,p.passage_id LIMIT 8) p),'[]'::jsonb),
  'nodes_truncated',(SELECT count(*)>20 FROM atom_page_links l WHERE l.semantic_atom_id=a.semantic_atom_id AND l.graph_id=a.graph_id),
  'citations_truncated',(SELECT count(*)>8 FROM semantic_atom_passages p WHERE p.semantic_atom_id=a.semantic_atom_id AND p.source_version_id=a.source_version_id AND p.graph_id=a.graph_id),
  'session',(SELECT jsonb_build_object('graph_id',a.graph_id,'provider_namespace',sv.provider_namespace,'session_id',sv.session_id) FROM library_session_versions(a.graph_id) sv WHERE sv.version_id=a.source_version_id AND sv.session_id IS NOT NULL),
  '_id',a.semantic_atom_id,'_at',a.captured_at
 )||'{}'::jsonb) ORDER BY a.occurred_at DESC NULLS LAST,a.captured_at DESC,a.semantic_atom_id DESC) FROM atom_page a),'[]'::jsonb)),
 'nodes',jsonb_build_object('total',(SELECT count(*) FROM matched_nodes),'items',COALESCE((SELECT jsonb_agg(jsonb_build_object(
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
 ) ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC) FROM node_page n),'[]'::jsonb)),
 'prominent_work',COALESCE((SELECT jsonb_agg(jsonb_build_object(
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
 ) ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC) FROM work_page n),'[]'::jsonb),
 'facets',jsonb_build_object(
  'services',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value,'value',value,'label',null,'source_count',source_count)) FROM (SELECT * FROM service_facets LIMIT 100) f),'[]'::jsonb),
  'content_kinds',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value,'value',value,'label',null,'source_count',source_count)) FROM (SELECT * FROM kind_facets LIMIT 100) f),'[]'::jsonb),
  'connections',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value::text,'value',value,'label',null,'source_count',source_count)) FROM (SELECT * FROM connection_facets LIMIT 100) f),'[]'::jsonb),
  'containers',COALESCE((SELECT jsonb_agg(jsonb_build_object('key',value::text,'value',value,'label',label,'source_count',source_count)) FROM (SELECT * FROM container_facets LIMIT 100) f),'[]'::jsonb),
  'unknown_origin_count',(SELECT count(*) FROM matched_sources WHERE origin IS NULL),
  'truncated',(SELECT count(*)>100 FROM service_facets) OR (SELECT count(*)>100 FROM kind_facets) OR (SELECT count(*)>100 FROM connection_facets) OR (SELECT count(*)>100 FROM container_facets)))
