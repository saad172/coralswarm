-- Migration: 0016_activity_types
-- Description: Extend activity_events.event_type CHECK constraint to include
--              'atom_ingested' (ingest pipeline) and 'node_updated' (node CRUD).
--
-- Down:
--   ALTER TABLE activity_events DROP CONSTRAINT activity_events_event_type_check;
--   ALTER TABLE activity_events ADD CONSTRAINT activity_events_event_type_check
--       CHECK (event_type IN ('atom_created', 'query_run', 'node_created', 'graph_created'));

ALTER TABLE activity_events DROP CONSTRAINT IF EXISTS activity_events_event_type_check;

ALTER TABLE activity_events
    ADD CONSTRAINT activity_events_event_type_check
    CHECK (event_type IN (
        'atom_created',
        'query_run',
        'node_created',
        'graph_created',
        'atom_ingested',
        'node_updated'
    ));
