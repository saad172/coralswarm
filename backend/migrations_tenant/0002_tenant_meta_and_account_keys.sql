-- 0002_tenant_meta_and_account_keys.sql
-- Per-tenant metadata + the relocated wrapped DEK. One tenant DB serves exactly
-- one org, so both tables are effectively singletons/small.

-- Single-row org fingerprint. Before publishing a tenant pool, the backend
-- asserts `SELECT org_id FROM tenant_meta` equals the org it believes it
-- connected to (swapped-DSN / two-orgs-one-DB defense, PR 1.1). The boolean-PK
-- + CHECK pattern enforces at most one row.
CREATE TABLE tenant_meta (
    id            boolean PRIMARY KEY DEFAULT true,
    org_id        text NOT NULL,          -- Clerk org id, verbatim = tenant_databases.clerk_org_id
    schema_notes  text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT tenant_meta_singleton CHECK (id = true)
);

-- Wrapped per-account DEK, relocated here from control-plane accounts.dek_wrapped
-- so a control-plane compromise yields zero content-decryption keys. account_id
-- is a bare uuid (accounts lives in the control DB). Provisioned by
-- onboard_tenant.sh; rotated_at set on rotation.
CREATE TABLE account_keys (
    account_id    uuid PRIMARY KEY,
    dek_wrapped   bytea NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    rotated_at    timestamptz
);
