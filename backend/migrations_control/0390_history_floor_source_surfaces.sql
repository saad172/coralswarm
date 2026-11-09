-- PAIRED WITH: tenant 0205 (history_floor_source_surfaces).
-- Team-seat history floors on sources and meeting transcripts (#1545).
--
-- 0204 put the floor on the atom read boundaries. A source item, its versions'
-- raw text and a meeting transcript carry the same words as the atoms 0204
-- hides, so they get the same floor. These are the self-contained forms of the
-- one predicate, for reads that hold only the item or transcript row. Rust
-- builds every call through `crate::history_floor`; callers pass the floors as
-- the uncorrelated `(SELECT public.reader_history_floors())`, one InitPlan per
-- statement, exactly as for 0204's `history_floor_admits`.
--
-- Each function returns true at once when `floors` is NULL or holds no floor
-- for the row's graph (every reader in production today), so an unfloored
-- read pays one jsonb probe and never the moment lookup.
--
-- 1. `history_floor_admits_source_item(g, current_version, created, floors)`:
--    an item's moment is its current version's `COALESCE(occurred_at,
--    captured_at)`, the semantic-atom moment 0204 uses, or the item's
--    `created_at` when it has no current version. A current version from
--    after the floor whose words began before it is still floored at its own
--    moment: a re-capture is a new version with a new moment, never a
--    rewrite of the old one.
--
-- 2. `history_floor_admits_transcript(g, meeting, current_version, created,
--    floors)`: a transcript's moment is the EARLIEST of its bound version's
--    moment, its row's `created_at`, and its chunk atoms' `"timestamp"`s (the
--    exact `source_id` and its numeric `:chunk:N` range, the bounds
--    `routes::meetings::list_meetings` already uses, migration 0074's
--    text_pattern_ops index). Earliest, because the text holds every one of
--    those atoms: a transcript is shown only when every atom it carries would
--    be. A chunk atom with no timestamp makes the moment NULL, which a floor
--    hides (fail closed, as 0204's `history_floor_admits` does).
--
-- `history_floor_admits_source_item` is SECURITY INVOKER with no SET clause:
-- source versions are not windowed per reader, and the caller has already
-- authorized the graph.
--
-- `history_floor_admits_transcript` is SECURITY DEFINER (search_path pinned).
-- It must see every chunk atom's time, and under the app role 0064's atoms
-- policy hides exactly the pre-floor atoms from a floored reader: an invoker
-- function then took the earliest VISIBLE atom, found it after the floor and
-- admitted the transcript (history_floor_surfaces_pg, app-role run). It
-- returns one boolean about the caller's own floors, and true at once for a
-- graph the caller has no floor on, so it discloses nothing about rows the
-- caller cannot read.

CREATE OR REPLACE FUNCTION public.history_floor_admits_source_item(g UUID, current_version UUID, created TIMESTAMPTZ, floors JSONB) RETURNS BOOLEAN
 LANGUAGE sql STABLE PARALLEL SAFE AS $$
 SELECT floors IS NULL OR NOT (floors ? (g::text)) OR public.history_floor_admits(g, COALESCE(
  (SELECT COALESCE(v.occurred_at, v.captured_at) FROM public.source_versions v WHERE v.graph_id = g AND v.version_id = current_version),
  created), floors)
$$;

CREATE OR REPLACE FUNCTION public.history_floor_admits_transcript(g UUID, meeting TEXT, current_version UUID, created TIMESTAMPTZ, floors JSONB) RETURNS BOOLEAN
 LANGUAGE sql STABLE PARALLEL SAFE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT floors IS NULL OR NOT (floors ? (g::text)) OR public.history_floor_admits(g, (
  SELECT CASE WHEN bool_or(x.moment IS NULL AND x.atom) THEN NULL ELSE LEAST(created, MIN(x.moment)) END FROM (
   SELECT COALESCE(v.occurred_at, v.captured_at) moment, false atom FROM public.source_versions v
    WHERE v.graph_id = g AND v.version_id = current_version
   UNION ALL
   SELECT a."timestamp", true FROM public.atoms a WHERE a.graph_id = g AND a.source_id = meeting
   UNION ALL
   SELECT a."timestamp", true FROM public.atoms a WHERE a.graph_id = g
    AND a.source_id ~>=~ (meeting || ':chunk:') AND a.source_id ~<~ (meeting || ':chunk;')
    AND substring(a.source_id FROM char_length(meeting) + 8) ~ '^[0-9]+$'
  ) x), floors)
$$;

REVOKE ALL ON FUNCTION public.history_floor_admits_source_item(UUID, UUID, TIMESTAMPTZ, JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.history_floor_admits_transcript(UUID, TEXT, UUID, TIMESTAMPTZ, JSONB) FROM PUBLIC;
DO $$ DECLARE r TEXT; BEGIN
 FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
   EXECUTE format('GRANT EXECUTE ON FUNCTION public.history_floor_admits_source_item(UUID, UUID, TIMESTAMPTZ, JSONB), public.history_floor_admits_transcript(UUID, TEXT, UUID, TIMESTAMPTZ, JSONB) TO %I', r);
  END IF;
 END LOOP;
END $$;
