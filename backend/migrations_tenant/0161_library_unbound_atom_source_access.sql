-- Library permission: gate unbound atoms on their source item's access state.
--
-- library_permitted_atoms admitted every atom with source_evidence_bound=false
-- unconditionally. That flag is set only when a source_version_chunks row is
-- inserted, and positional chunk atoms ({source_id}:chunk:{n}, inserted with
-- ON CONFLICT DO NOTHING) get a chunk row only on first insert. Many atoms of
-- a captured source item therefore stay unbound, and revoking or losing access
-- to that item (access_state <> 'permitted') did not hide them.
--
-- An unbound atom now belongs to the item in the same graph with the same
-- source_type whose source_id equals the atom's source_id with one trailing
-- ':chunk:<digits>' removed. Every capture path writes exactly that shape
-- (routes::ingest, Slack events, GitHub backfill), and it agrees with the
-- chunk-row mapping for every bound atom. When any such item is not permitted
-- the atom is denied (fail closed across provider namespaces, which atoms do
-- not record). Atoms with no such item (legacy rows that predate source
-- items, notes, MCP add_context, coding/agent sessions captured without an
-- item) have no access state to consult and keep their existing behavior.
-- Bound atoms are unchanged.
--
-- The gate is a top-level NOT EXISTS against the partial index below, so the
-- whole-graph branch plans as a hash anti join over the (usually tiny) set of
-- non-permitted items, and the id-list branch as one index probe per atom.
-- The suffix strip is spelled with rtrim rather than
-- regexp_replace(source_id,':chunk:[0-9]+$','') (same result, cheaper per row).
--
-- Owner, SECURITY INVOKER and grants are preserved by CREATE OR REPLACE; the
-- search_path set in 0157 is restated because CREATE OR REPLACE resets it.
CREATE INDEX source_items_denied_identity ON public.source_items(graph_id,source_type,source_id)
 WHERE access_state<>'permitted';

CREATE OR REPLACE FUNCTION public.library_permitted_atoms(g UUID,atom_ids UUID[] DEFAULT NULL) RETURNS TABLE(atom_id UUID)
 LANGUAGE SQL STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 WITH mapped AS MATERIALIZED (
  SELECT c.atom_id,bool_or(s.access_state='permitted') permitted,bool_or(s.access_state<>'permitted') denied
  FROM public.source_version_chunks c JOIN public.source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
  JOIN public.source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE c.graph_id=g AND atom_ids IS NULL GROUP BY c.atom_id)
 SELECT a.atom_id FROM public.atoms a LEFT JOIN mapped m ON m.atom_id=a.atom_id
 WHERE a.graph_id=g AND atom_ids IS NULL
  AND NOT a.evidence_retracted AND NOT a.derived_copy_bound
  AND (NOT a.source_evidence_bound OR COALESCE(m.permitted,false)) AND NOT COALESCE(m.denied,false)
  AND NOT EXISTS(SELECT 1 FROM public.source_items s
   WHERE NOT a.source_evidence_bound AND s.graph_id=g AND s.access_state<>'permitted' AND s.source_type=a.source_type
    AND s.source_id=CASE WHEN right(rtrim(a.source_id,'0123456789'),7)=':chunk:' AND rtrim(a.source_id,'0123456789')<>a.source_id
     THEN left(rtrim(a.source_id,'0123456789'),-7) ELSE a.source_id END)
 UNION ALL
 SELECT a.atom_id FROM public.atoms a CROSS JOIN LATERAL (
  SELECT bool_or(s.access_state='permitted') permitted,bool_or(s.access_state<>'permitted') denied
  FROM public.source_version_chunks c JOIN public.source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
  JOIN public.source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE c.graph_id=g AND c.atom_id=a.atom_id) m
 WHERE a.graph_id=g AND a.atom_id=ANY(atom_ids)
  AND NOT a.evidence_retracted AND NOT a.derived_copy_bound
  AND (NOT a.source_evidence_bound OR COALESCE(m.permitted,false)) AND NOT COALESCE(m.denied,false)
  AND NOT EXISTS(SELECT 1 FROM public.source_items s
   WHERE NOT a.source_evidence_bound AND s.graph_id=g AND s.access_state<>'permitted' AND s.source_type=a.source_type
    AND s.source_id=CASE WHEN right(rtrim(a.source_id,'0123456789'),7)=':chunk:' AND rtrim(a.source_id,'0123456789')<>a.source_id
     THEN left(rtrim(a.source_id,'0123456789'),-7) ELSE a.source_id END)
$$;
