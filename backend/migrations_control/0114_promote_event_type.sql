-- Migration: 0114_promote_event_type
-- Description: Extend activity_events.event_type CHECK to include 'promote'
--   (today-zoomable-journal audit gap #7,
--   .scratch/today-zoomable-journal/research/hourly-completeness.md).
--   Promoting a reef/atoms into another ocean previously had no event type
--   at all, so the CHECK roster admitted no way to record it and the act of
--   promoting was invisible as a journal "interaction" line. Emitted at
--   promote time with `Some(target_graph_id)` and a payload of
--   `{promoted_atom_count, source_graph_id}` — see
--   `routes::promote`/`mcp::tools::write`'s promote handler.
--
-- Down:
--   ALTER TABLE activity_events DROP CONSTRAINT IF EXISTS activity_events_event_type_check;
--   ALTER TABLE activity_events ADD CONSTRAINT activity_events_event_type_check
--       CHECK (event_type IN ('atom_created', 'query_run', 'node_created', 'graph_created',
--                             'atom_ingested', 'node_updated', 'supersession_created'));

ALTER TABLE activity_events DROP CONSTRAINT IF EXISTS activity_events_event_type_check;

ALTER TABLE activity_events
    ADD CONSTRAINT activity_events_event_type_check
    CHECK (event_type IN (
        'atom_created',
        'query_run',
        'node_created',
        'graph_created',
        'atom_ingested',
        'node_updated',
        'supersession_created',
        'promote'
    ));
