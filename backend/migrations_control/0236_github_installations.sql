-- Migration: 0236_github_installations
-- Description: GitHub connector (PR A, sync plumbing) — the provider-specific
--   side table `connector_installs` deliberately does NOT carry (contract
--   §5.3: "routing tuples only, never secrets" — and, per that same section,
--   provider-specific metadata belongs in a side table, the precedent
--   `slack_integrations` sets for Slack). `connector_installs` still gets its
--   own `provider = 'github'` row per installation
--   (`external_workspace_id = installation_id` as text — see
--   `connectors::github::record_installation`) for I8 tenant routing, exactly
--   as docs/connectors/github.md §3.3 specifies; THIS table is what the
--   webhook handler and the installs-listing endpoint actually read for
--   suspend state and install metadata, keyed the same way.
--
--   docs/connectors/github.md §9 divergence entry: the design doc does not
--   name this table explicitly. It exists because `connector_installs` has
--   no `suspended`/`account_login`/`account_type` columns (by design — it is
--   provider-blind), and the webhook path (I8: "unknown/suspended → 202,
--   nothing written") needs to answer "is this installation known, and is it
--   suspended" from a single indexed lookup, not a live GitHub API round trip
--   on every inbound event.
--
--   No RLS, deliberately — same posture as `connector_installs`
--   (migration 0121's own header) and for the identical reason: the webhook
--   handler resolves `installation.id` -> `account_id` BEFORE any tenant/
--   account scope exists to set (I8's whole point — resolve the tenant
--   before touching tenant data). `installation_id` is globally unique (one
--   GitHub installation binds to exactly one CoralSwarm account, set once at
--   the OAuth callback — docs/connectors/github.md §3.1) so, unlike
--   `connector_installs` (which can have several account rows share one
--   provider+workspace under its own documented multi-account shape), this
--   table's lookup is a plain UNIQUE index, no SECURITY DEFINER resolver
--   function needed.
--
--   `account_login`/`account_type` are refreshed opportunistically (the
--   installs-listing endpoint updates them from a live
--   `GET /app/installations/{id}` call — see that route's own doc) rather
--   than being load-bearing routing data; a stale value here never causes a
--   misroute, only a stale display label.
--
-- Depends on: 0001_init (accounts, users), 0121_connector_installs (the
--             "no RLS, pre-tenant-resolution bootstrap table" precedent).
-- Down:
--   DROP TABLE IF EXISTS github_installations;

CREATE TABLE IF NOT EXISTS github_installations (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    installation_id   BIGINT      NOT NULL,
    account_login     TEXT        NOT NULL DEFAULT '',
    -- 'Organization' | 'User' — GitHub's own `installation.account.type`.
    account_type      TEXT        NOT NULL DEFAULT '',
    connected_by      UUID        REFERENCES users(id) ON DELETE SET NULL,
    suspended_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (installation_id)
);

CREATE INDEX IF NOT EXISTS idx_github_installations_account
    ON github_installations (account_id);

-- `coralswarm_app` already has SELECT, INSERT, UPDATE, DELETE on this new
-- table via 0083's ALTER DEFAULT PRIVILEGES (no fresh GRANT needed here) —
-- same note every RLS-bearing migration in this tree carries; this table
-- happens to need no RLS policy at all (see this file's header) but still
-- benefits from the same blanket privilege grant `connector_installs` relies
-- on for its own unscoped reads.
