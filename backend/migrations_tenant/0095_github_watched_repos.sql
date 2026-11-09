-- Migration: 0095_github_watched_repos (tenant tree)
-- Description: Tenant-tree half of the paired
--   `migrations_control/0235_github_watched_repos.sql` — see that file for
--   the full rationale (column choices, the two-partial-index destination
--   key, the divergences from the design doc recorded in
--   docs/connectors/github.md §9). This `CREATE TABLE` must stay
--   byte-equivalent to the control side's or `scripts/check_schema_parity.sh`
--   fails.
--
--   `account_id`/`owner_user_id` carry NO foreign key here, unlike the
--   control side — `accounts`/`users` are control-only tables that do not
--   exist in a tenant database — same `slack_watched_channels`
--   (migrations_tenant/0013) precedent this mirrors. `graph_id` keeps its FK
--   to `graphs` (present on both trees).
--
--   RLS: anchored on `graph_id` via `tenant_rls_visible_graph_ids()`, same
--   shape as `slack_watched_channels`/`teams_watched_resources` — this table
--   IS meant to work on the org tenant plane (an org's own tenant database
--   is where its `graphs`/per-member/team oceans actually live). A pending
--   (NULL `graph_id`) row is invisible under this policy until it is routed
--   — the selection API (`routes::github::put_repos`) writes/reads it through
--   `tenancy::scope::begin_tenant_system_scoped`, the same "no single acting
--   member" scoping `ensure_slack_watched_channel` uses for its own writes.
--
-- Depends on: 0001_tenant_baseline, 0009_tenant_rls (coralswarm_tenant_app
--             role, tenant_rls_visible_graph_ids()).
-- Paired with: migrations_control/0235_github_watched_repos.sql.
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON github_watched_repos;
--   ALTER TABLE github_watched_repos DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS github_watched_repos;

CREATE TABLE IF NOT EXISTS github_watched_repos (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL,
    installation_id   BIGINT      NOT NULL,
    repo_id           BIGINT      NOT NULL,
    owner             TEXT        NOT NULL,
    name              TEXT        NOT NULL,
    default_branch    TEXT        NOT NULL DEFAULT '',
    visibility        TEXT        NOT NULL DEFAULT 'public',
    archived          BOOLEAN     NOT NULL DEFAULT false,
    fork              BOOLEAN     NOT NULL DEFAULT false,
    size_kb           BIGINT      NOT NULL DEFAULT 0,
    active            BOOLEAN     NOT NULL DEFAULT false,
    paused_reason     TEXT,
    graph_id          UUID        REFERENCES graphs(id) ON DELETE CASCADE,
    owner_user_id     UUID,
    destination_kind  TEXT        CHECK (destination_kind IN ('personal', 'team', 'org')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_synced_at    TIMESTAMPTZ,
    head_tree_sha     TEXT,
    narrative_cursor  JSONB,
    code_cursor       JSONB,
    backfill_job_id   UUID,
    backfill_progress JSONB,
    sync_stopped_at   TIMESTAMPTZ,

    CONSTRAINT github_watched_repos_destination_check CHECK (
        (graph_id IS NULL AND destination_kind IS NULL)
        OR (graph_id IS NOT NULL AND destination_kind IS NOT NULL)
    ),
    CONSTRAINT github_watched_repos_active_requires_graph CHECK (
        active = false OR graph_id IS NOT NULL
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_github_watched_repos_pending
    ON github_watched_repos (account_id, installation_id, repo_id)
    WHERE graph_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_github_watched_repos_routed
    ON github_watched_repos (account_id, installation_id, repo_id, graph_id)
    WHERE graph_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_github_watched_repos_installation
    ON github_watched_repos (account_id, installation_id);

ALTER TABLE github_watched_repos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON github_watched_repos;
-- NULL-friendly (unlike a bare slack_watched_channels-style `graph_id IN
-- (...)`), and deliberately so: unlike Slack's tenant-plane table (whose own
-- header notes it has "no live path that could attempt to write a NULL-
-- graph_id row" on this plane), a pending github_watched_repos row (§3.3,
-- granted by GitHub but not yet routed) genuinely can be written on the org
-- tenant plane by the selection API. `x IN (subquery)` is NULL-UNFRIENDLY —
-- a bare `graph_id IN (...)` would make a NULL-graph_id row invisible AND
-- unwritable even under `begin_tenant_system_scoped`, since the NULL sits on
-- the LEFT of the membership test regardless of what the subquery returns.
-- Every pending row is visible account-wide (there is no ocean yet to scope
-- it to); a routed row falls through to the ordinary member-ocean check.
CREATE POLICY member_ocean_isolation ON github_watched_repos
    USING (
        graph_id IS NULL
        OR graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    );
