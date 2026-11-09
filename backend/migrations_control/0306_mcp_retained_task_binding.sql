-- Scoped MCP tasks share the retained Ask receipt engine in both protocol eras.
-- Existing rows remain NULL/unavailable. Requester and owning scope account are
-- separate typed UUIDs: control identities must never FK across tenant databases.
ALTER TABLE mcp_tasks ADD COLUMN library_scope JSONB CHECK(library_scope IS NULL OR (jsonb_typeof(library_scope)='object' AND octet_length(library_scope::text)<=16384));
ALTER TABLE mcp_tasks ADD COLUMN scope_owner_account_id UUID;
ALTER TABLE mcp_tasks ADD COLUMN answer_receipt_id UUID;
ALTER TABLE mcp_tasks ADD COLUMN result_sha256 TEXT CHECK(result_sha256 IS NULL OR result_sha256 ~ '^[0-9a-f]{64}$');
ALTER TABLE mcp_tasks ADD CONSTRAINT mcp_scoped_binding CHECK((library_scope IS NULL AND scope_owner_account_id IS NULL AND answer_receipt_id IS NULL AND result_sha256 IS NULL) OR (library_scope IS NOT NULL AND scope_owner_account_id IS NOT NULL AND (status<>'completed' OR (answer_receipt_id IS NOT NULL AND result_sha256 IS NOT NULL))));
-- P8 reconciliation consumes answer_support_receipts and must purge result/error
-- copies for affected tasks; expiration/saved-read gating is not physical purge.
CREATE INDEX mcp_scoped_receipt ON mcp_tasks(answer_receipt_id) WHERE answer_receipt_id IS NOT NULL;

CREATE FUNCTION mcp_retained_binding_immutable() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.library_scope IS NOT NULL AND (NEW.library_scope IS DISTINCT FROM OLD.library_scope OR NEW.scope_owner_account_id IS DISTINCT FROM OLD.scope_owner_account_id OR NEW.account_id<>OLD.account_id OR NEW.owner_user_id<>OLD.owner_user_id OR NEW.graph_id<>OLD.graph_id OR NEW.tool_name<>OLD.tool_name OR NEW.idempotency_request_hash IS DISTINCT FROM OLD.idempotency_request_hash) THEN
  RAISE EXCEPTION 'retained task binding is immutable' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION mcp_retained_binding_immutable() FROM PUBLIC;
CREATE TRIGGER mcp_retained_binding_immutable BEFORE UPDATE ON mcp_tasks FOR EACH ROW EXECUTE FUNCTION mcp_retained_binding_immutable();
