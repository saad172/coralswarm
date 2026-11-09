-- PAIRED WITH: tenant 0200 (library_session_versions_fallback, byte-identical body).
-- GET /v1/sessions hit the 10 s statement timeout on the org tenant (chain
-- health paged http_5xx on 2026-10-27; 18.6 s measured read-only on prod).
-- Half of that cost was in this function: its chunk-derived session fallback
-- joined EVERY coding_session of the graph against each version's chunk atoms
-- with a LIKE, and the planner put coding_sessions on the outer side, so one
-- evaluation on a graph with 5.7k versions and 765 sessions ran 4M LIKE
-- probes and 476k heap fetches to resolve nothing (the other half, the list
-- query evaluating this function once per page row, is fixed in
-- store/mod.rs's coding_sessions_list_pg).
--
-- Same result, atoms first. Only a chunk atom that is a `cc:` capture looks
-- for its sessions, through a LATERAL the planner cannot flatten (the LIMIT
-- blocks subquery pull-up, so the join order is fixed with the atom as the
-- driver). LIMIT 2 does not change the answer: two matching sessions for one
-- atom already make count(DISTINCT) >= 2, which the CASE maps to NULL
-- ("ambiguous") exactly as before. Session ids may contain ':' (ingest only
-- bounds their length), so the prefix match stays a LIKE on the escaped id
-- rather than a split_part equality.
--
-- Prod equivalence (read-only, graph ac9befa1, 2026-10-27): 5,709 versions,
-- 478 resolved, 0 mismatches against the 0304 body. The list statement went
-- from 18,636 ms to 512 ms with both halves applied.
--
-- Plain STABLE SQL (not SECURITY DEFINER, no search_path pin, single SELECT so
-- it still inlines; library_read_pg's cor94_session_key_lookup_inlines_the_
-- resolver_function guards that). EXECUTE grants survive CREATE OR REPLACE.
CREATE OR REPLACE FUNCTION library_session_versions(g UUID) RETURNS TABLE(
 version_id UUID,source_item_id UUID,provider_namespace TEXT,session_id TEXT,captured_at TIMESTAMPTZ)
 LANGUAGE SQL STABLE AS $$
 SELECT v.version_id,v.source_item_id,v.provider_namespace,
 COALESCE(NULLIF(v.capture_context->>'session_id',''),NULLIF(v.source_metadata->>'session_id',''),
  (SELECT CASE WHEN count(DISTINCT m.session_id)=1 THEN min(m.session_id) END
   FROM source_version_chunks c
   JOIN atoms a ON a.graph_id=c.graph_id AND a.atom_id=c.atom_id
   CROSS JOIN LATERAL (
    SELECT cs.session_id FROM coding_sessions cs
    WHERE cs.graph_id=a.graph_id
     AND a.source_id LIKE ('cc:'||replace(replace(replace(cs.session_id,'\','\\'),'%','\%'),'_','\_')||':%')
    LIMIT 2) m
   WHERE c.graph_id=v.graph_id AND c.source_version_id=v.version_id
    AND a.source_type IN('coding_session','agent_session','work_session','meeting_session','mcp')
    AND a.source_id LIKE 'cc:%' AND library_atom_permitted(g,a.atom_id))),v.captured_at
 FROM source_versions v JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
 WHERE v.graph_id=g AND s.access_state='permitted'
$$;
