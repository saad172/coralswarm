-- Migration 0091: capture boundary for message-scope opt-in saves (options train).
-- Nullable preserves every existing whole-thread watch's behavior. A message-
-- scope save records its request-start timestamp here; live-follow recovery
-- must not silently import messages that predate that save. Both providers'
-- incremental paths consult the boundary; deletion propagation ignores it.
-- Paired across control and tenant because both watched tables exist in both.
-- Down: ALTER TABLE gmail_watched_threads DROP COLUMN follow_since;
--       ALTER TABLE outlook_watched_conversations DROP COLUMN follow_since;
ALTER TABLE gmail_watched_threads ADD COLUMN IF NOT EXISTS follow_since TIMESTAMPTZ;
ALTER TABLE outlook_watched_conversations ADD COLUMN IF NOT EXISTS follow_since TIMESTAMPTZ;
