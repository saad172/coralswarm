-- Migration: 0078_tenant_databases
-- Description: Control-plane routing table for database-per-org tenancy. Maps a
--   Clerk organization (and its subdomain slug) to the per-org tenant Postgres
--   that holds all of that org's content. Lives ONLY in the control tree — this
--   is the routing authority the backend consults on every org request to pick
--   which tenant pool to use (PR 1.1 TenantRegistry / PR 1.2 binding gate).
--
--   Isolation-critical invariants (enforced here, not just in app code):
--     * `slug` UNIQUE + a strict charset CHECK — the slug is parsed from the
--       request Host and interpolated into a Fly secret name (TENANT_DB_<SLUG>)
--       and DNS, so it must be a safe, lowercase, hyphenated token.
--     * `db_secret_ref` UNIQUE — the NAME of a Fly secret holding the tenant
--       DSN, never the raw DSN (credentials never live in this table). Two orgs
--       can never point at the same secret (a second guard beyond the tenant-DB
--       fingerprint check).
--     * `status` CHECK — only 'active' is servable by the gate; 'provisioning'
--       (mid-onboarding), 'suspended' (offboarding write-freeze), and
--       'offboarded' (kept for audit) all reject at the routing layer.
--
--   `account_id` FKs `accounts(id)` (both are control-plane) so a routing row
--   cannot reference a nonexistent account; ON DELETE RESTRICT because offboarding
--   keeps a tombstoned account + an 'offboarded' routing row for audit rather
--   than hard-deleting.
CREATE TABLE tenant_databases (
    clerk_org_id   TEXT        PRIMARY KEY,
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    slug           TEXT        NOT NULL,
    db_secret_ref  TEXT        NOT NULL,
    region         TEXT,
    substrate      TEXT        NOT NULL DEFAULT 'neon',
    schema_version BIGINT,
    status         TEXT        NOT NULL DEFAULT 'provisioning',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tenant_databases_slug_key       UNIQUE (slug),
    CONSTRAINT tenant_databases_secret_ref_key UNIQUE (db_secret_ref),
    -- lowercase alnum + internal hyphens, 3-32 chars, no leading/trailing hyphen
    CONSTRAINT tenant_databases_slug_charset
        CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$'),
    CONSTRAINT tenant_databases_status_valid
        CHECK (status IN ('provisioning', 'active', 'suspended', 'offboarded'))
);

-- The gate's hot path resolves a routing row by slug (from Host) on every org
-- request; the worker loop and orchestrator enumerate active tenants.
CREATE INDEX tenant_databases_status_idx ON tenant_databases (status);
