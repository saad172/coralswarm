-- Migration 0066: google_watch_channels.owner_user_id + gmail_watched_threads
--   — tenant twin of migrations_control/0186_google_gmail_watch_owner_and_
--   threads.sql (gsuite Train G2 PR 3, schema-parity twin pattern per m365
--   Train 2 PR 2.2/#661's precedent, same as 0180/0062 and 0182/0063).
--
--   RENUMBERED from the originally-announced tenant 0065 after #683 merged
--   holding that number for `coding_sessions.session_kind`'s own tenant
--   twin — merged wins; see the control migration's own header. Purely a
--   rename — no content change.
--
--   Byte-identical column set to the control-tree migration; see that
--   migration's header for the full column-by-column rationale. Structural
--   differences required by the tenant-tree convention
--   (`migrations_tenant/0001_tenant_baseline.sql`'s own header: cross-DB
--   references are bare uuid columns with no FK — users/accounts live in the
--   control DB only):
--
--   * `google_watch_channels.owner_user_id` — bare `UUID`, no
--     `REFERENCES users(id)` (users is control-plane-only).
--   * `gmail_watched_threads.account_id`/`ingested_by` — bare `UUID`, no FK.
--   * `gmail_watched_threads.graph_id` — a REAL FK to `graphs(id)`, since
--     `graphs` exists on the tenant tree too (same as
--     `outlook_watched_conversations.graph_id`'s own tenant-tree FK).
--
--   RLS: `member_ocean_isolation` anchored on `graph_id` via
--   `tenant_rls_visible_graph_ids()` — same shape as
--   `outlook_watched_conversations`'s own tenant-plane policy
--   (`migrations_tenant/0009_tenant_rls.sql:1272`). Lands in all four RLS
--   rosters, see the control migration's header.
--
--   No standalone `(account_id)` index — see the control migration's header
--   (CodeRabbit round 1 finding #10 on PR #686): kept in parity with that
--   decision here too, since `check_schema_parity.sh` diffs these two trees
--   structurally.
--
-- Depends on: 0062_google_watch_channels.sql, 0009_tenant_rls.sql (tenant_rls_visible_graph_ids()).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON gmail_watched_threads;
--   DROP INDEX IF EXISTS idx_gmail_watched_threads_backfill_pending;
--   DROP TABLE IF EXISTS gmail_watched_threads;
--   ALTER TABLE google_watch_channels DROP COLUMN IF EXISTS owner_user_id;

ALTER TABLE google_watch_channels
    ADD COLUMN IF NOT EXISTS owner_user_id UUID;

CREATE TABLE IF NOT EXISTS gmail_watched_threads (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id         UUID        NOT NULL,
    graph_id           UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    ingested_by        UUID        NOT NULL,
    thread_id          TEXT        NOT NULL,
    backfill_cursor    TEXT,
    backfill_complete  BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, thread_id)
);

CREATE INDEX IF NOT EXISTS idx_gmail_watched_threads_backfill_pending
    ON gmail_watched_threads (account_id)
    WHERE NOT backfill_complete;

ALTER TABLE gmail_watched_threads ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON gmail_watched_threads;
CREATE POLICY member_ocean_isolation ON gmail_watched_threads
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
