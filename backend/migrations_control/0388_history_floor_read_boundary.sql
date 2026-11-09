-- PAIRED WITH: tenant 0204 (history_floor_read_boundary).
-- Team-seat history floors on the read boundaries (#1525).
--
-- `team_seat_grants.history_from` (0064) was enforced only by the `atoms`
-- SELECT policy. Production tenant requests and workers connect as the table
-- owner, which bypasses RLS, and the request path never switches role
-- (tenancy/scope.rs, 0159). So a floored member's reads were not windowed.
-- The owner connection stays. The floor is applied by the read boundaries the
-- request path already goes through, in this one shared predicate:
--
--   public.history_floor_admits(graph, moment, (SELECT public.reader_history_floors()))
--
-- 1. `reader_history_floors()`: the calling reader's floors, as a jsonb object
--    of graph id to floor, or NULL when the reader has none. On the tenant
--    plane it delegates to `tenant_rls_visible_graph_windows()` (0064), so
--    the grant rules are not restated: NULL for a system actor, a non-team
--    graph, a member with no grant, or a member with any NULL grant;
--    otherwise the MIN of their grants' `history_from`. The control plane has
--    no windows function and no floors (0184's header), so it returns NULL
--    there. One body on both planes: the plane is tested at run time, the
--    same idiom as 0155/0323. The plpgsql body is parsed for syntax only at
--    CREATE, so the tenant-only call is never resolved on control.
--    The first probe returns before the windows walk when the actor holds no
--    windowed grant, which is every reader today (0 rows in production).
--    SECURITY DEFINER so the owner and the app role read the same grants.
--    No floor inside a trigger. 0064 windows reads only, and a write's
--    trigger maintenance (reef identity re-derivation, purge signals) is
--    global state computed by SECURITY DEFINER functions that RLS never
--    windowed. Under the owner connection the floor is keyed on
--    `app.user_id`, not on the role, so without this rule a floored member's
--    DELETE would recompute other readers' reef titles from that member's
--    window (reef_identity_pg's asymmetric-window case).
--
-- 2. `history_floor_admits(g, moment, floors)`: true unless `floors` holds a
--    floor for `g` and `moment` is before it. A NULL moment is before every
--    floor (fail closed, as 0064's `timestamp >= floor` is for a NULL
--    timestamp) and visible when there is no floor. SQL, no SET clause and
--    no subquery, so the planner inlines it; callers pass the floors as an
--    uncorrelated `(SELECT public.reader_history_floors())`, one InitPlan per
--    statement, so each row costs one jsonb lookup.
--
-- 3. Legacy atoms: the moment is `atoms."timestamp"`, as in 0064's policy.
--    `library_permitted_atoms` is the P7 read boundary: Library search, Ask,
--    the store's scoped reads (`library_atom_permitted`), MCP get_atom and the
--    atom routes all admit legacy atoms through it. 0161's body, unchanged
--    except for the floor predicate on both branches.
--
-- 4. Semantic atoms (founder decision, 2026-10-28: floors cover semantic
--    atoms too): the moment is the source version's
--    `COALESCE(occurred_at, captured_at)`, the observed time the rest of the
--    code uses for a semantic atom (fact_supersession's `observed_at`).
--    `served_semantic_atoms` is "one view for every semantic-atom reader"
--    (0183): the source evidence routes, workspace reads, reef lexical and
--    disagreements read it. 0183's view, unchanged except for the floor.
--    Library's own scope CTE reads `semantic_atoms` directly and applies the
--    same predicate in Rust (`crate::history_floor`).
--
-- What this does not touch: `nodes`, edges, `source_items`, `source_versions`
-- and raw transcripts are not windowed, as 0064 left them.
-- The RLS policies are unchanged.
--
-- Workers run in system scope, so `reader_history_floors()` is NULL for them
-- and the Library evidence they cache is unchanged.

CREATE INDEX IF NOT EXISTS idx_team_seat_grants_windowed_user
    ON team_seat_grants (user_id) WHERE history_from IS NOT NULL;

CREATE OR REPLACE FUNCTION public.reader_history_floors() RETURNS jsonb
 LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
 actor uuid;
 floors jsonb;
BEGIN
 IF pg_trigger_depth() > 0 OR to_regprocedure('public.tenant_rls_visible_graph_windows()') IS NULL THEN
  RETURN NULL;
 END IF;
 actor := NULLIF(current_setting('app.user_id', true), '')::uuid;
 IF actor IS NULL OR NOT EXISTS (
  SELECT 1 FROM public.team_seat_grants g WHERE g.user_id = actor AND g.history_from IS NOT NULL
 ) THEN
  RETURN NULL;
 END IF;
 SELECT jsonb_object_agg(w.graph_id::text, w.history_floor) INTO floors
  FROM public.tenant_rls_visible_graph_windows() w
 WHERE w.history_floor IS NOT NULL;
 RETURN floors;
END $$;

CREATE OR REPLACE FUNCTION public.history_floor_admits(g UUID, moment TIMESTAMPTZ, floors JSONB) RETURNS BOOLEAN
 LANGUAGE sql STABLE PARALLEL SAFE AS $$
 SELECT COALESCE(moment, '-infinity'::timestamptz) >= COALESCE((floors ->> (g::text))::timestamptz, '-infinity'::timestamptz)
$$;

REVOKE ALL ON FUNCTION public.reader_history_floors() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.history_floor_admits(UUID, TIMESTAMPTZ, JSONB) FROM PUBLIC;
DO $$ DECLARE r TEXT; BEGIN
 FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
   EXECUTE format('GRANT EXECUTE ON FUNCTION public.reader_history_floors(), public.history_floor_admits(UUID, TIMESTAMPTZ, JSONB) TO %I', r);
  END IF;
 END LOOP;
END $$;

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
  AND public.history_floor_admits(a.graph_id,a."timestamp",(SELECT public.reader_history_floors()))
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
  AND public.history_floor_admits(a.graph_id,a."timestamp",(SELECT public.reader_history_floors()))
$$;

CREATE OR REPLACE VIEW served_semantic_atoms WITH (security_invoker = true) AS
SELECT a.*, v.source_item_id,
       COALESCE(a.source_version_id = s.current_version_id, false) AS source_version_is_current
  FROM semantic_atoms a
  JOIN source_versions v ON v.graph_id = a.graph_id AND v.version_id = a.source_version_id
  JOIN source_items s ON s.graph_id = v.graph_id AND s.source_item_id = v.source_item_id
 WHERE s.access_state = 'permitted'
   AND (a.extraction_version = v.extraction_version OR v.extraction_status <> 'done')
   AND public.history_floor_admits(v.graph_id, COALESCE(v.occurred_at, v.captured_at), (SELECT public.reader_history_floors()));
