-- Authentication metadata only; no provider tokens, tool output or tenant FK.
CREATE TABLE composio_connections (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
 owner_user_id UUID NOT NULL,
 service TEXT NOT NULL CHECK(service IN ('linear','jira')),
 connected_account_id TEXT NOT NULL CHECK(length(connected_account_id) BETWEEN 1 AND 256),
 auth_config_id TEXT NOT NULL CHECK(length(auth_config_id) BETWEEN 1 AND 256),
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','active','reconnect','paused')),
 provider_namespace TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(account_id,owner_user_id,connected_account_id)
);
ALTER TABLE composio_connections ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON composio_connections USING(account_id=current_setting('app.account_id')::uuid) WITH CHECK(account_id=current_setting('app.account_id')::uuid);
-- Content-side selection and resumable checkpoint, scoped to one member graph.
-- connection_id is an opaque control-plane reference, deliberately no cross-DB FK.
CREATE TABLE composio_capture_scopes (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 account_id UUID NOT NULL,
 owner_user_id UUID NOT NULL,
 connection_id UUID,
 service TEXT NOT NULL CHECK(service IN ('linear','jira')),
 mode TEXT NOT NULL CHECK(mode IN ('composio','synthetic')),
 provider_namespace TEXT NOT NULL CHECK(length(provider_namespace) BETWEEN 1 AND 512),
 project_id TEXT NOT NULL CHECK(length(project_id) BETWEEN 1 AND 256),
 project_name TEXT NOT NULL DEFAULT '',
 auto_capture BOOLEAN NOT NULL DEFAULT false,
 cursor TEXT,
 state TEXT NOT NULL DEFAULT 'ready' CHECK(state IN ('ready','running','caught_up','paused','retry','reconnect')),
 captured_items BIGINT NOT NULL DEFAULT 0 CHECK(captured_items>=0),
 last_capture_at TIMESTAMPTZ,
 retry_at TIMESTAMPTZ,
 error_code TEXT,
 lease_id UUID,
 lease_until TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(graph_id,service,provider_namespace,project_id),
 CHECK((mode='synthetic' AND provider_namespace LIKE 'demo:%' AND connection_id IS NULL)
    OR (mode='composio' AND provider_namespace NOT LIKE 'demo:%' AND connection_id IS NOT NULL))
);
CREATE INDEX composio_scopes_owner ON composio_capture_scopes(account_id,owner_user_id,updated_at DESC);
ALTER TABLE composio_capture_scopes ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON composio_capture_scopes USING(account_id=current_setting('app.account_id')::uuid) WITH CHECK(account_id=current_setting('app.account_id')::uuid);
-- Pre-scope bootstrap returns only routing identifiers, never provider content.
CREATE FUNCTION resolve_composio_capture_due()
RETURNS TABLE(id UUID,account_id UUID,owner_user_id UUID,service TEXT,refresh BOOLEAN)
LANGUAGE SQL SECURITY DEFINER SET search_path=public,pg_temp AS $$
 SELECT c.id,c.account_id,c.owner_user_id,c.service,c.state='caught_up'
 FROM composio_capture_scopes c JOIN graphs g ON g.id=c.graph_id AND g.account_id=c.account_id
 WHERE c.mode='composio' AND c.auto_capture AND c.state NOT IN ('paused','reconnect')
   AND g.archived_at IS NULL AND (g.kind='personal' OR (g.kind='member' AND g.owner_user_id=c.owner_user_id))
   AND (c.lease_until IS NULL OR c.lease_until<now())
   AND (c.retry_at IS NULL OR c.retry_at<=now())
   AND (c.last_capture_at IS NULL OR c.last_capture_at<now()-interval '30 minutes')
 ORDER BY c.last_capture_at NULLS FIRST LIMIT 10;
$$;
REVOKE ALL ON FUNCTION resolve_composio_capture_due() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_composio_capture_due() TO coralswarm_app;
