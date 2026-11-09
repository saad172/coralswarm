-- Additive observation metadata. Existing identities/history remain untouched.
-- Item origin is the latest supplied attribution; version origin is the capture
-- observation and is never rewritten merely because a connection changes.
ALTER TABLE source_items ADD COLUMN origin JSONB
 CHECK(origin IS NULL OR (jsonb_typeof(origin)='object' AND octet_length(origin::text)<=16384));
ALTER TABLE source_versions ADD COLUMN origin JSONB
 CHECK(origin IS NULL OR (jsonb_typeof(origin)='object' AND octet_length(origin::text)<=16384));
-- Existing account/member-ocean RLS policies also protect these columns.
-- No credential-plane FK and no historical metadata backfill.
