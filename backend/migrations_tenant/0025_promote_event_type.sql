-- Migration: 0025_promote_event_type
-- Description: Extend activity_events.event_type CHECK to include 'promote'
--   (today-zoomable-journal audit gap #7). The TENANT half of control's
--   0114_promote_event_type.sql — see that migration's header for the full
--   rationale.
-- Depends on: 0001_tenant_baseline (activity_events)
-- PAIRED WITH: migrations_control/0114_promote_event_type.sql — the DDL in
--              the two files is IDENTICAL and must stay that way
--              (`scripts/check_schema_parity.sh`).
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
