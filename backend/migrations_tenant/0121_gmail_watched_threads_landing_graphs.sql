-- Track the oceans where the connector put replies before the owner changes
-- their primary. A UUID array has no foreign keys. Deleted graph IDs are
-- harmless because the graph lookup finds no atoms.
-- Seed the current primary for existing followed threads. Older original
-- imports are recovered by source identity and attribution when hidden or deleted.
-- Control twin 0284_gmail_watched_threads_landing_graphs.sql.

ALTER TABLE gmail_watched_threads
    ADD COLUMN IF NOT EXISTS landing_graph_ids UUID[] NOT NULL DEFAULT '{}';

UPDATE gmail_watched_threads w
   SET landing_graph_ids = ARRAY[g.id]
  FROM graphs g
 WHERE w.follow_replies
   AND g.account_id = w.account_id
   AND g.is_primary = true
   AND g.archived_at IS NULL
   AND (g.kind = 'personal' OR (g.kind = 'member' AND g.owner_user_id = w.ingested_by))
   AND g.id <> w.graph_id
   AND cardinality(w.landing_graph_ids) = 0;
