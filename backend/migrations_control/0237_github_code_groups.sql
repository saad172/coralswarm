-- Migration: 0237_github_code_groups
-- Description: docs/connectors/github.md §3.10 — the summary tier (PR B
--   part 2). One row per detected "group" (a language-native package found
--   from its manifest, or a directory fallback) plus one per-repo rollup
--   row (`kind = 'repo'`), driving the `github_code_summary` atoms and the
--   Jev staleness gate/force-after guard.
--
--   Paired with migrations_tenant/0096_github_code_groups.sql — one of the
--   shared content tables, so this must stay byte-equivalent to that file's
--   `CREATE TABLE` or `scripts/check_schema_parity.sh` fails.
--
--   ── Why a table, not JSONB on github_watched_repos ──
--   §3.10's regeneration gate needs per-group, independently-mutating state
--   (`unjudged_changes`, `last_verdict`, `last_judged_at`) that a webhook
--   handler updates ONE group at a time, concurrently with the backfill's
--   own progressive summarization tick touching a DIFFERENT group of the
--   same repo. A single JSONB column on `github_watched_repos` (the way
--   `code_cursor`/`narrative_cursor` work) would mean read-modify-write
--   races between those two writers for the same repo, and no way to index
--   "which groups are over the force-after threshold" across every repo for
--   the reconcile sweep without deserializing every row's JSON. A table
--   gives per-group row locking and a real index for both.
--
--   ── Columns ──
--   `watched_repo_id` — the repo this group belongs to.
--   `graph_id` — carried on the row (not just reachable via `watched_repo_id`
--   -> `github_watched_repos.graph_id`) so the tenant-plane RLS policy can
--   anchor directly on it the same way `github_watched_repos`' own policy
--   does, without a join. ALWAYS set (unlike `github_watched_repos.graph_id`,
--   which is nullable for a not-yet-routed pending row): a group is only
--   ever detected for an ACTIVE, routed watch row (§3.6 "after Part 1's code
--   phase completes" — that phase never runs before `graph_id` is set, see
--   `connectors::github_backfill::run_tick`'s own `let Some(graph_id) = ...`
--   early return), so there is no "pending group" state to represent.
--
--   `path` — `.` for the repo-root directory-fallback group (loose top-level
--   files with no owning manifest); `''` (empty string) is RESERVED for the
--   one `kind = 'repo'` row per watched repo (the whole-repo rollup) — a
--   distinct value from `.` is required because `UNIQUE (watched_repo_id,
--   path)` would otherwise collide the root directory group with the repo
--   rollup, both of which are real, simultaneously-existing rows. Every
--   other group's `path` is its manifest or fallback directory's repo-
--   relative path with no leading/trailing slash.
--
--   `kind` — `'package'` (a language-native manifest was found), `'directory'`
--   (the fallback, merged-upward/split-downward bucketing), or `'repo'` (the
--   single per-repo rollup row, §3.10's last sentence: "rebuilt only when a
--   group summary actually changed").
--
--   `files_digest` — a stable hash (sha256 hex) of the group's member files:
--   for a `package`/`directory` group, the sorted `(path, blob_sha)` pairs of
--   its current member files; for the `repo` row, the sorted `(group.path,
--   group.summarized_files_digest)` pairs of every non-repo group — so the
--   repo row's own digest changes exactly when some group's SUMMARIZED
--   state changes, never on every raw file edit, which is what makes the
--   repo summary "rebuilt only when a group summary actually changed" a
--   mechanical fact rather than a rule the worker has to remember.
--   `summarized_files_digest` — the digest the CURRENT `last_summary_text`/
--   atom was generated from; `summary_source_id IS NULL OR
--   summarized_files_digest IS DISTINCT FROM files_digest` is the exact
--   predicate `connectors::github_summary` uses everywhere for "this group
--   needs a fresh summary".
--
--   `last_summary_text` — a redundant copy of the generated summary's own
--   prose (NOT authoritative — the ingested `github_code_summary` atom is;
--   this is a cache). Exists for two internal readers that would otherwise
--   need to read an atom's own encrypted R2 content back out from a
--   background-worker context — a plumbing path (per-account DEK unwrap
--   outside the request path) that does not exist anywhere in this tree
--   today: (1) the Jev staleness judge's "current_summary" input
--   (`decision::judge::judge_github_summary_staleness`), and (2) the repo-
--   level rollup's own digest text (§3.10, "one atom per repo rolled up from
--   the GROUP summaries, their text, capped"). Recorded as a divergence in
--   docs/connectors/github.md §9.
--
--   `unjudged_changes` — incremented once per touched-and-not-rewritten pass
--   (§3.10); reset to 0 whenever this group's summary is actually rewritten,
--   whether because Jev said `stale`, `JEV_SUMMARY_GATE_MODE=off`/`shadow`
--   always rewrote, or the force-after guard
--   (`workers::github_reconcile_sweep`) fired.
--   `last_verdict`/`last_judged_at` — the most recent Jev Choice outcome,
--   for observability; `NULL` when the gate mode is `off` (no judgment ever
--   ran) or no touch has happened yet.
--   `last_summarized_at` — when `last_summary_text`/the atom were last
--   written.
--
-- Depends on: 0001_init (accounts, users), 0005 (graphs), 0235_github_watched_repos.
-- Paired with: migrations_tenant/0096_github_code_groups.sql.
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON github_code_groups;
--   ALTER TABLE github_code_groups DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS github_code_groups;

CREATE TABLE IF NOT EXISTS github_code_groups (
    id                       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id               UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
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

-- The force-after guard's own index (`workers::github_reconcile_sweep`):
-- "every group at/over the threshold, across every repo" without a
-- sequential scan.
CREATE INDEX IF NOT EXISTS idx_github_code_groups_unjudged
    ON github_code_groups (watched_repo_id, unjudged_changes);

-- `coralswarm_app` already has SELECT, INSERT, UPDATE, DELETE on this new
-- table via 0083's ALTER DEFAULT PRIVILEGES (no fresh GRANT needed here) —
-- same note `0235_github_watched_repos`'s own header carries.
ALTER TABLE github_code_groups ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON github_code_groups;
-- Same shape as github_watched_repos' policy (0235), simplified: `graph_id`
-- is NOT NULL on this table (see the header above), so there is no
-- pending/NULL-graph_id branch to carry here.
CREATE POLICY account_isolation ON github_code_groups
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );
