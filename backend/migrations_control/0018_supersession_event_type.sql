-- Migration: 0018_supersession_event_type
-- Description: Extend activity_events.event_type CHECK to include 'supersession_created'
--
-- Down:
--   ALTER TABLE activity_events DROP CONSTRAINT IF EXISTS activity_events_event_type_check;
--   ALTER TABLE activity_events ADD CONSTRAINT activity_events_event_type_check
--       CHECK (event_type IN ('atom_created', 'query_run', 'node_created', 'graph_created',
--                             'atom_ingested', 'node_updated'));

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
        'supersession_created'
    ));
