-- Migration: 0096_github_code_groups (tenant tree)
-- Description: Tenant-tree half of the paired
--   `migrations_control/0237_github_code_groups.sql` — see that file for
--   the full rationale (column choices, the `.`-vs-`''` path convention,
--   `last_summary_text`'s divergence from the design doc). This
--   `CREATE TABLE` must stay byte-equivalent to the control side's or
--   `scripts/check_schema_parity.sh` fails.
--
--   `account_id` carries NO foreign key here, unlike the control side —
--   `accounts` is a control-only table that does not exist in a tenant
--   database — same `github_watched_repos` (migrations_tenant/0095)
--   precedent this mirrors. `watched_repo_id`/`graph_id` keep their FKs
--   (`github_watched_repos`/`graphs`, present on both trees).
--
--   RLS: anchored DIRECTLY on `graph_id` via `tenant_rls_visible_graph_ids()`
--   — same shape as `github_watched_repos`' own tenant policy, simplified
--   (no NULL-friendly branch: `graph_id` is NOT NULL on this table, see the
--   control migration's header for why).
--
-- Depends on: 0001_tenant_baseline, 0009_tenant_rls (coralswarm_tenant_app
--             role, tenant_rls_visible_graph_ids()), 0095_github_watched_repos.
-- Paired with: migrations_control/0237_github_code_groups.sql.
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON github_code_groups;
--   ALTER TABLE github_code_groups DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS github_code_groups;

CREATE TABLE IF NOT EXISTS github_code_groups (
    id                       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id               UUID        NOT NULL,
    watched_repo_id          UUID        NOT NULL REFERENCES github_watched_repos(id) ON DELETE CASCADE,
    graph_id                 UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    path                     TEXT        NOT NULL,
    kind                     TEXT        NOT NULL CHECK (kind IN ('package', 'directory', 'repo')),
    language                 TEXT,
    file_count               INT         NOT NULL DEFAULT 0,
    files_digest             TEXT        NOT NULL DEFAULT '',
    summary_source_id        TEXT,
    summarized_files_digest  TEXT,
    last_summary_text        TEXT,
    unjudged_changes         INT         NOT NULL DEFAULT 0,
    last_verdict             TEXT        CHECK (last_verdict IN ('stale', 'current')),
    last_judged_at           TIMESTAMPTZ,
    last_summarized_at       TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT github_code_groups_unjudged_nonneg CHECK (unjudged_changes >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_github_code_groups_repo_path
    ON github_code_groups (watched_repo_id, path);

CREATE INDEX IF NOT EXISTS idx_github_code_groups_account
    ON github_code_groups (account_id);

CREATE INDEX IF NOT EXISTS idx_github_code_groups_unjudged
    ON github_code_groups (watched_repo_id, unjudged_changes);

ALTER TABLE github_code_groups ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON github_code_groups;
CREATE POLICY member_ocean_isolation ON github_code_groups
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
