-- PAIRED WITH: control 0352 (served_semantic_atoms).
-- v1 extraction removal, step 2: one view for every semantic-atom reader.
-- Paired tenant/control migrations with byte-identical bodies; only the
-- header names the pair.
--
-- 1. `served_semantic_atoms`: the semantic atoms a reader may show. An atom
--    is served when its source item is permitted and it belongs to its
--    version's pinned extraction, or that extraction has not finished yet.
--    Re-extraction (v1 -> v2/v3, v2 -> v3) retires the earlier profile's
--    atoms when its last window commits (`v2::retire_superseded_tx`), but
--    keeps any that an answer receipt, a temporal record or a supersession
--    edge cites. Those survivors stay in `semantic_atoms` for their citers
--    and are not served next to the atoms that replaced them. A version
--    still pinned to `source-atoms-v1` is served from its v1 atoms: nothing
--    has replaced them. While a re-extraction is pending, running or failed,
--    the earlier atoms are all the version has, so they stay served.
--
--    `security_invoker`: the caller's RLS on `semantic_atoms`,
--    `source_versions` and `source_items` applies, exactly as when it joins
--    them itself. The column list is `semantic_atoms` as of this migration
--    plus the item and whether the atom's version is the item's current one.
CREATE VIEW served_semantic_atoms WITH (security_invoker = true) AS
SELECT a.*, v.source_item_id,
       COALESCE(a.source_version_id = s.current_version_id, false) AS source_version_is_current
  FROM semantic_atoms a
  JOIN source_versions v ON v.graph_id = a.graph_id AND v.version_id = a.source_version_id
  JOIN source_items s ON s.graph_id = v.graph_id AND s.source_item_id = v.source_item_id
 WHERE s.access_state = 'permitted'
   AND (a.extraction_version = v.extraction_version OR v.extraction_status <> 'done');

DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_app') THEN
  GRANT SELECT ON served_semantic_atoms TO coralswarm_app;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
  GRANT SELECT ON served_semantic_atoms TO coralswarm_tenant_app;
 END IF;
END $$;
