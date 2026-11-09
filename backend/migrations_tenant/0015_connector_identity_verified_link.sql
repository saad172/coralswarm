-- Migration: 0015_connector_identity_verified_link
-- Paired with migrations_control/0104_connector_identity_verified_link.sql —
-- see that file's header for the full rationale (C5 phase one, org-plane-only
-- verified-email matching, `user_emails` deliberately dropped).
--
-- `user_id` is a BARE uuid column here, with no FK: `users` is
-- control-plane-only (org-tenancy split), so there is nothing on this plane
-- to reference — same treatment as every other cross-plane column on this
-- table's neighbors (account_id, owner_id, ingested_by, ...; see
-- migrations_tenant/0001_tenant_baseline.sql's header: "Cross-DB references
-- ... are bare uuid columns with no FK").
--
-- check_schema_parity.sh CASCADE-drops `users` from the control-tree scratch
-- DB before diffing, which strips 0104's FK down to exactly this bare-column
-- shape — the two migrations are expected to differ ONLY in that FK clause;
-- every other column/constraint here is byte-identical to 0104's.
--
-- No RLS here either, for the same reason 0104 adds none — see that file's
-- header.
--
-- Depends on: 0001_tenant_baseline (connector_identities)
-- Down:
--   ALTER TABLE connector_identities
--       DROP CONSTRAINT IF EXISTS connector_identities_link_provenance_chk,
--       DROP CONSTRAINT IF EXISTS connector_identities_link_method_chk,
--       DROP COLUMN IF EXISTS link_verified_at,
--       DROP COLUMN IF EXISTS link_method,
--       DROP COLUMN IF EXISTS user_id;

ALTER TABLE connector_identities
    ADD COLUMN IF NOT EXISTS user_id UUID,
    ADD COLUMN IF NOT EXISTS link_method TEXT,
    ADD COLUMN IF NOT EXISTS link_verified_at TIMESTAMPTZ,
    ADD CONSTRAINT connector_identities_link_method_chk
        CHECK (link_method IS NULL OR link_method IN ('verified_email')),
    ADD CONSTRAINT connector_identities_link_provenance_chk
        CHECK (
            (user_id IS NULL AND link_method IS NULL AND link_verified_at IS NULL)
            OR
            (user_id IS NOT NULL AND link_method IS NOT NULL AND link_verified_at IS NOT NULL)
        );
