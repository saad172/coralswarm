-- Connections the capture worker parked because their member left the
-- account, whose Composio revocation has not succeeded yet, for one plane
-- (p_org NULL = consumer accounts, otherwise the org's account). updated_at is
-- the last attempt; the wait before the next one doubles with the time since
-- disconnect, from 1 minute up to 6 hours. A member's own Disconnect is never
-- retried here while they are still a member. Returns routing identifiers only.
CREATE FUNCTION resolve_composio_departed_unrevoked(p_org TEXT,p_limit INT)
RETURNS TABLE(id UUID,account_id UUID,owner_user_id UUID,updated_at TIMESTAMPTZ)
LANGUAGE SQL STABLE SECURITY DEFINER SET search_path=public,pg_temp AS $$
 SELECT c.id,c.account_id,c.owner_user_id,c.updated_at
 FROM composio_connections c JOIN accounts a ON a.id=c.account_id
 WHERE c.status='disconnected' AND c.revoked_at IS NULL
   AND NOT EXISTS(SELECT 1 FROM account_members m WHERE m.account_id=c.account_id AND m.user_id=c.owner_user_id)
   AND c.updated_at<=now()-least(interval '6 hours',greatest(interval '1 minute',c.updated_at-coalesce(c.disconnected_at,c.updated_at)))
   AND CASE WHEN p_org IS NULL
        THEN NOT EXISTS(SELECT 1 FROM tenant_databases td WHERE td.account_id=c.account_id)
        ELSE a.clerk_org_id=p_org END
 ORDER BY c.updated_at,c.id LIMIT greatest(p_limit,0);
$$;
REVOKE ALL ON FUNCTION resolve_composio_departed_unrevoked(TEXT,INT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_composio_departed_unrevoked(TEXT,INT) TO coralswarm_app;
CREATE INDEX composio_connections_unrevoked ON composio_connections(updated_at) WHERE status='disconnected' AND revoked_at IS NULL;
