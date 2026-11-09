-- Backfill structured channel provenance for Teams corals ingested before the
-- Microsoft/Teams funnel started stamping channel_id/channel_name (migration
-- 0034 added the columns; Teams ingest left them NULL). Without this, the
-- Sources view groups Teams corals under their raw container GUID with no human
-- label. We recover the label from the watched-resource the user opted in.
--
-- A Teams atom's source_id is "{container_id}:{message_id}:chunk:N". Teams
-- container ids themselves contain colons (e.g. "19:abc@thread.tacv2"), so we
-- match by exact prefix (container_id + ':') rather than splitting on ':'.
-- author_id/author_name cannot be recovered here (the author GUID was never
-- stored for historical Teams atoms); they fill in for messages ingested from
-- now on.

UPDATE atoms a
SET channel_id   = w.channel_id,
    channel_name = NULLIF(w.display_name, '')
FROM teams_watched_resources w
WHERE a.source_type = 'teams_channel'
  AND a.graph_id    = w.graph_id
  AND w.kind        = 'channel'
  AND w.channel_id <> ''
  AND a.channel_name IS NULL
  AND left(a.source_id, length(w.channel_id) + 1) = w.channel_id || ':';

UPDATE atoms a
SET channel_id   = w.chat_id,
    channel_name = NULLIF(w.display_name, '')
FROM teams_watched_resources w
WHERE a.source_type = 'teams_chat'
  AND a.graph_id    = w.graph_id
  AND w.kind        = 'chat'
  AND w.chat_id    <> ''
  AND a.channel_name IS NULL
  AND left(a.source_id, length(w.chat_id) + 1) = w.chat_id || ':';
