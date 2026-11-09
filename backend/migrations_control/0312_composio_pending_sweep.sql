-- Pending Composio connections finish server-side: the capture worker checks
-- each one for its own plane (p_org NULL = consumer accounts, otherwise the
-- org's account), oldest-checked first. updated_at doubles as "last checked",
-- so one connection is not checked more than about twice a minute.
-- Returns routing identifiers only.
CREATE FUNCTION resolve_composio_pending_connections(p_org TEXT,p_limit INT)
RETURNS TABLE(id UUID,account_id UUID,owner_user_id UUID,service TEXT)
LANGUAGE SQL STABLE SECURITY DEFINER SET search_path=public,pg_temp AS $$
 SELECT c.id,c.account_id,c.owner_user_id,c.service
 FROM composio_connections c JOIN accounts a ON a.id=c.account_id
 WHERE c.status='pending'
   AND c.updated_at<=now()-interval '30 seconds'
   AND CASE WHEN p_org IS NULL
        THEN NOT EXISTS(SELECT 1 FROM tenant_databases td WHERE td.account_id=c.account_id)
        ELSE a.clerk_org_id=p_org END
 ORDER BY c.updated_at,c.id LIMIT greatest(p_limit,0);
$$;
REVOKE ALL ON FUNCTION resolve_composio_pending_connections(TEXT,INT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_composio_pending_connections(TEXT,INT) TO coralswarm_app;
CREATE INDEX composio_connections_pending ON composio_connections(updated_at) WHERE status='pending';
