-- Composio "connect and done": one workspace-wide capture per connection,
-- polled for changes after a watermark set at connect. Tenant 0146 carries the
-- same scope change; connections live only here.
ALTER TABLE composio_connections
 DROP CONSTRAINT composio_connections_service_check,
 DROP CONSTRAINT composio_connections_status_check;
UPDATE composio_connections SET status=CASE status WHEN 'reconnect' THEN 'reconnect_needed' WHEN 'paused' THEN 'needs_attention' ELSE status END;
ALTER TABLE composio_connections
 -- Any recipe toolkit; the registry, not the schema, decides which exist.
 ADD CONSTRAINT composio_connections_service_check CHECK(service ~ '^[a-z0-9_]{1,64}$'),
 ADD CONSTRAINT composio_connections_status_check CHECK(status IN ('pending','active','needs_attention','reconnect_needed','disconnected')),
 -- Recipe self-check on this connection's real data. The first 'passed' for a
 -- toolkit takes it out of beta (composio_verified_toolkits()).
 ADD COLUMN self_check TEXT NOT NULL DEFAULT 'pending' CHECK(self_check IN ('pending','passed','failed')),
 ADD COLUMN self_check_reason TEXT CHECK(length(self_check_reason)<=200),
 ADD COLUMN self_checked_at TIMESTAMPTZ,
 ADD COLUMN disconnected_at TIMESTAMPTZ,
 -- Composio confirmed deletion and provider revocation of the connected account.
 ADD COLUMN revoked_at TIMESTAMPTZ;
CREATE INDEX composio_connections_verified ON composio_connections(service) WHERE self_check='passed';
-- Cross-account by design: beta status is a property of the toolkit. Returns
-- slugs only.
CREATE FUNCTION composio_verified_toolkits()
RETURNS SETOF TEXT LANGUAGE SQL STABLE SECURITY DEFINER SET search_path=public,pg_temp AS $$
 SELECT DISTINCT service FROM composio_connections WHERE self_check='passed';
$$;
REVOKE ALL ON FUNCTION composio_verified_toolkits() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION composio_verified_toolkits() TO coralswarm_app;
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
GRANT EXECUTE ON FUNCTION resolve_composio_capture_due() TO coralswarm_app;
