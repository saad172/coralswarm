-- Migration: 0235_github_watched_repos
-- Description: docs/connectors/github.md §3.3/§3.4 — the per-repo watch +
--   destination table for the GitHub connector (PR A, sync plumbing). Mirrors
--   `slack_watched_channels` (migrations_control/0101, migrations_tenant/0013)
--   in spirit: a repository is GitHub's container (contract §1), granted by
--   the App install screen but never auto-indexed — a row here is what turns
--   a granted repo into an actively-synced one, and where it lands.
--
--   Paired with migrations_tenant/0095_github_watched_repos.sql — one of the
--   shared content tables, so this must stay byte-equivalent to that file's
--   `CREATE TABLE` or `scripts/check_schema_parity.sh` fails.
--
--   ── Divergence from the design doc's literal column list (recorded in
--   docs/connectors/github.md §9, same PR) ──
--   The doc's §3.3 gives `UNIQUE (account_id, installation_id, repo_id)`, but
--   §3.4 also requires "the same repo selected by two different people into
--   two different oceans is two watch rows". Those two sentences conflict
--   unless `graph_id` is part of the unique key — see the constraint section
--   below for the two-partial-index shape this resolves it with (the same
--   `oauth_tokens`/`connector_installs` "account-wide vs per-owner" split
--   already uses for an analogous reason).
--
--   ── Columns ──
--   `installation_id`/`repo_id` — GitHub's own numeric ids (BIGINT: GitHub
--   issues 64-bit ids for both). `owner`/`name` — the repo's `login/name`
--   pair at last sync, kept denormalized for display without a live API call.
--   `default_branch`/`visibility`/`archived`/`fork`/`size_kb` — declared
--   properties of the container (contract's Code-host archetype note: a
--   public repo's visibility is what decides destination eligibility, never
--   its roster).
--
--   `graph_id` — the destination ocean, same semantics as
--   `slack_watched_channels.graph_id`: NULL until a human (or the "pending"
--   landing state below) has one. UNLIKE Slack's `mode`/nullable-`graph_id`
--   pair (which distinguishes an explicit fan-out pick from "nobody has
--   chosen yet"), GitHub has no fan-out destination at all (§3.4: personal /
--   team / org, always exactly one ocean) — so here NULL means exactly one
--   thing: "granted by GitHub, not yet routed" (§3.3's "repos granted later
--   land as paused rows" — `paused_reason = 'pending_selection'`). Once a
--   destination is chosen the row's `graph_id` is set and never NULLed again
--   (changing a destination is a migration, contract I2 — same rule the two
--   partial unique indexes below enforce structurally).
--
--   `owner_user_id` — DIVERGES from `slack_watched_channels.user_id`
--   (`NOT NULL`, provenance-only): here it is NULLABLE, because a row can
--   exist before any human has acted (the "granted, not yet selected" pending
--   state has no selecting user at all). Once selected, this is the human who
--   picked the destination — provenance only, same as Slack's column; never a
--   read predicate.
--
--   `destination_kind` — `'personal' | 'team' | 'org'` (§3.4), NULL exactly
--   when `graph_id` is NULL (same pairing the CHECK below enforces).
--
--   `narrative_cursor`/`code_cursor` — per-repo, per-object-type backfill/
--   reconcile cursor state (§3.6), opaque JSONB the ingest workers (PR B)
--   own the shape of; this migration only reserves the columns.
--   `backfill_job_id` — the most recent `JobKind::GithubBackfill` row for
--   this repo, if any is running/ran.
--   `backfill_progress` — DIVERGES from the design doc, which assumed a
--   generic `job_progress` mechanism; no such mechanism exists in this tree
--   today (`jobs` carries no progress column — see `jobs::queue::JobRow`).
--   Recorded in docs/connectors/github.md §9. Shape reserved for PR B/the
--   worker that actually drives backfill; this PR only writes `null`.
--   `head_tree_sha` — the default branch's HEAD tree, as of the last
--   successful code-tier sync (§3.7's reconcile-sweep comparison target).
--   `sync_stopped_at`/`paused_reason` — §3.8's "never purge" pause state:
--   uninstall, repo removed from the grant, or an admin toggle-off all set
--   `active = false` here and keep every atom.
--
-- Depends on: 0001_init (accounts, users), 0005 (graphs), 0101_slack_watched_channels
--             (the paired-migration + RLS-anchor precedent this mirrors).
-- Paired with: migrations_tenant/0095_github_watched_repos.sql.
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON github_watched_repos;
--   ALTER TABLE github_watched_repos DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS github_watched_repos;

CREATE TABLE IF NOT EXISTS github_watched_repos (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
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
    -- Destination ocean. NULL = granted but not yet routed (§3.3 "pending").
    graph_id          UUID        REFERENCES graphs(id) ON DELETE CASCADE,
    -- Provenance only (the human who picked the destination); NULLABLE
    -- unlike slack_watched_channels.user_id — see this file's header.
    owner_user_id     UUID        REFERENCES users(id) ON DELETE SET NULL,
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

    -- `graph_id` and `destination_kind` are set together or not at all.
    CONSTRAINT github_watched_repos_destination_check CHECK (
        (graph_id IS NULL AND destination_kind IS NULL)
        OR (graph_id IS NOT NULL AND destination_kind IS NOT NULL)
    ),
    -- Active requires a destination — a repo cannot be actively syncing into
    -- nowhere.
    CONSTRAINT github_watched_repos_active_requires_graph CHECK (
        active = false OR graph_id IS NOT NULL
    )
);

-- At most one PENDING (not yet routed) row per repo — see this file's
-- header for why `graph_id` can legitimately be NULL and why a plain
-- `UNIQUE (account_id, installation_id, repo_id, graph_id)` alone would NOT
-- prevent duplicate pending rows (Postgres treats every NULL as distinct
-- for uniqueness purposes).
CREATE UNIQUE INDEX IF NOT EXISTS uq_github_watched_repos_pending
    ON github_watched_repos (account_id, installation_id, repo_id)
    WHERE graph_id IS NULL;

-- At most one row per repo PER DESTINATION ocean, once routed — §3.4's "two
-- different people, two different oceans, two watch rows" resolved by
-- including graph_id in the key (docs/connectors/github.md §9 divergence
-- entry; the design doc's literal `UNIQUE (account_id, installation_id,
-- repo_id)` would have made that impossible).
CREATE UNIQUE INDEX IF NOT EXISTS uq_github_watched_repos_routed
    ON github_watched_repos (account_id, installation_id, repo_id, graph_id)
    WHERE graph_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_github_watched_repos_installation
    ON github_watched_repos (account_id, installation_id);

-- `coralswarm_app` already has SELECT, INSERT, UPDATE, DELETE on this new
-- table via 0083's ALTER DEFAULT PRIVILEGES (no fresh GRANT needed here) —
-- same note `0101_slack_watched_channels`'s own header carries.
ALTER TABLE github_watched_repos ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON github_watched_repos;
-- Same shape as slack_watched_channels' policy (migration 0101): READ is a
-- direct, unambiguous `account_id` anchor; WRITE additionally requires
-- `graph_id` (when set) to belong to a graph owned by the SAME account, and
-- is NULL-friendly for the pending-row case (a NULL graph_id has nothing to
-- own-check — the CHECK constraints above already keep a NULL graph_id
-- paired with a NULL destination_kind and active=false).
CREATE POLICY account_isolation ON github_watched_repos
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND (
            graph_id IS NULL
            OR graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
    );
