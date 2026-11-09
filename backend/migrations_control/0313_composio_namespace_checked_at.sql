-- When a poll last re-resolved the connection's workspace live and found it
-- equal to the bound provider_namespace. NULL means a check is due: never
-- checked, or a stream call failed on auth or contract since.
ALTER TABLE composio_connections ADD COLUMN namespace_checked_at TIMESTAMPTZ;
