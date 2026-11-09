-- Composio "connect and done": one workspace-wide capture per connection,
-- polled for changes after a watermark set at connect. Same scope change as
-- control 0311 (connections live only on the control plane).
-- Synthetic demo scopes retire with the demo; their evidence stays in the demo ocean.
DELETE FROM composio_capture_scopes WHERE mode='synthetic';
-- One workspace-wide capture per connection and ocean. Pilot project scopes
-- fold into the oldest one; every atom they captured stays where it is.
UPDATE composio_capture_scopes k SET captured_items=f.total
FROM (SELECT (array_agg(id ORDER BY created_at,id))[1] AS keep,sum(captured_items) AS total
      FROM composio_capture_scopes GROUP BY graph_id,connection_id) f
WHERE k.id=f.keep;
DELETE FROM composio_capture_scopes s WHERE EXISTS(
 SELECT 1 FROM composio_capture_scopes k
 WHERE k.graph_id=s.graph_id AND k.connection_id=s.connection_id AND (k.created_at,k.id)<(s.created_at,s.id));
ALTER TABLE composio_capture_scopes
 DROP CONSTRAINT composio_capture_scopes_check,
 DROP CONSTRAINT composio_capture_scopes_state_check,
 DROP CONSTRAINT composio_capture_scopes_service_check,
 DROP COLUMN mode,
 DROP COLUMN project_id,
 DROP COLUMN project_name,
 DROP COLUMN auto_capture,
 -- Items updated at or after the watermark are pending; set at connect, so
 -- nothing older is ever fetched for ingestion.
 ADD COLUMN watermark TIMESTAMPTZ NOT NULL DEFAULT now(),
 -- Start of the pass in progress; becomes the watermark when the pass ends.
 ADD COLUMN pass_started_at TIMESTAMPTZ,
 ADD COLUMN next_poll_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 ADD COLUMN last_change_at TIMESTAMPTZ;
-- Pilot scopes switch to workspace-wide from now; project cursors mean nothing there.
UPDATE composio_capture_scopes SET
 state=CASE state WHEN 'reconnect' THEN 'reconnect_needed' WHEN 'retry' THEN 'retry' WHEN 'paused' THEN 'needs_attention' ELSE 'active' END,
 cursor=NULL,watermark=now(),next_poll_at=now(),retry_at=NULL,lease_id=NULL,lease_until=NULL,updated_at=now();
ALTER TABLE composio_capture_scopes
 ALTER COLUMN connection_id SET NOT NULL,
 ALTER COLUMN state SET DEFAULT 'active',
 ADD CONSTRAINT composio_capture_scopes_service_check CHECK(service ~ '^[a-z0-9_]{1,64}$'),
 ADD CONSTRAINT composio_capture_scopes_real_namespace CHECK(provider_namespace NOT LIKE 'demo:%'),
 ADD CONSTRAINT composio_capture_scopes_state_check CHECK(state IN ('active','retry','needs_attention','reconnect_needed','disconnected')),
 ADD CONSTRAINT composio_capture_scopes_connection_key UNIQUE(graph_id,connection_id);
DROP FUNCTION resolve_composio_capture_due();
-- Pre-scope bootstrap returns only routing identifiers, never provider content.
CREATE FUNCTION resolve_composio_capture_due()
RETURNS TABLE(id UUID,account_id UUID,owner_user_id UUID,service TEXT)
LANGUAGE SQL SECURITY DEFINER SET search_path=public,pg_temp AS $$
 SELECT c.id,c.account_id,c.owner_user_id,c.service
 FROM composio_capture_scopes c JOIN graphs g ON g.id=c.graph_id AND g.account_id=c.account_id
 WHERE c.state IN ('active','retry')
   AND g.archived_at IS NULL AND (g.kind='personal' OR (g.kind='member' AND g.owner_user_id=c.owner_user_id))
   AND (c.lease_until IS NULL OR c.lease_until<now())
   AND c.next_poll_at<=now()
 ORDER BY c.next_poll_at LIMIT 10;
$$;
REVOKE ALL ON FUNCTION resolve_composio_capture_due() FROM PUBLIC;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='coralswarm_tenant_app') THEN
  GRANT EXECUTE ON FUNCTION resolve_composio_capture_due() TO coralswarm_tenant_app;
 END IF;
END $$;
