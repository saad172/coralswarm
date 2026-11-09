-- Migration 0067: microsoft_calendar_sync_state — tenant twin of
--   migrations_control/0189_microsoft_calendar_sync_state.sql (m365
--   enterprise train PR 3.8, "Calendar default-on for org members").
--
--   RENUMBERED from the plan-file's originally-announced tenant 0066: that
--   number was already taken by gsuite Train G2 PR 3's
--   `gmail_watched_threads` (`migrations_tenant/
--   0066_google_gmail_watch_owner_and_threads.sql`, merged as part of #686)
--   by the time this PR ran — merged wins, per the ledger's own precedent
--   (#683 vs #686, #686 vs #688). Control 0189 was unaffected (0188 is PR
--   3.2's control-only `microsoft_admin_consents`), so only this side moves.
--
--   Byte-identical column set to the control-tree migration; see that
--   migration's header for the full column-by-column rationale, in
--   particular why `graph_id` is `NOT NULL` (a row is written only once the
--   member's ocean resolves, never before — required for this policy below
--   to ever be satisfiable). Structural differences required by the
--   tenant-tree convention (`migrations_tenant/0001_tenant_baseline.sql`'s
--   own header: cross-DB references are bare uuid columns with no FK — users/
--   accounts live in the control DB only):
--
--   * `account_id` — bare `UUID`, no `REFERENCES accounts(id)`.
--   * `owner_user_id` — bare `UUID`, no `REFERENCES users(id)`.
--   * `graph_id` — a REAL FK to `graphs(id)`, since `graphs` exists on the
--     tenant tree too (same as `gmail_watched_threads.graph_id`'s own
--     tenant-tree FK).
--
--   RLS: `member_ocean_isolation` anchored on `graph_id` via
--   `tenant_rls_visible_graph_ids()` — same shape as
--   `gmail_watched_threads`'s own tenant-plane policy
--   (`migrations_tenant/0066_google_gmail_watch_owner_and_threads.sql`).
--
--   ── `WITH CHECK` (CodeRabbit round 1 on PR #692) ──
--   `gmail_watched_threads` (0066), named above as this migration's
--   structural precedent, carries `USING` only — Postgres then also applies
--   `USING` to writes, which on THIS table would let a write pair a
--   `graph_id` visible to the acting session with an `owner_user_id` that is
--   NOT that graph's real owner (e.g. member B pairing `owner_user_id = A`
--   with A's own `graph_id`, or any `graph_id` with an `owner_user_id` the
--   graph does not actually belong to). The control twin's own header
--   (`migrations_control/0189_microsoft_calendar_sync_state.sql`) explains
--   the full reasoning for closing this with a `graphs`-derived `WITH
--   CHECK`, mirroring `seat_grant_sources` (0184 control / 0064 tenant) —
--   read that header for the ownership-subquery rationale; only the tenant-
--   tree specifics are called out here.
--
--   `graphs.owner_user_id`/`kind`/`archived_at` all exist on THIS tree's
--   `graphs` too (0005/0090's byte-identical column set — see 0005's own
--   header), so the subquery is identical in shape to the control side
--   except for `account_id`: dropped from the predicate rather than
--   compared to a `current_setting('app.account_id')`, because the tenant
--   tree has no such GUC (`tenant_rls_actor_user_id()`/`tenant_rls_is_
--   system()` are the only two this database's RLS reads — 0009 §2) and a
--   tenant database holds exactly one account by construction (ADR-0007) —
--   comparing `graphs.account_id` to `microsoft_calendar_sync_state.
--   account_id` directly (both bare, unvalidated `UUID` columns on this
--   tree) is the same invariant restated without a GUC to anchor it to.
--   Because this subquery runs as the SAME `coralswarm_tenant_app` role the
--   write itself runs as, it is additionally filtered by `graphs`' own
--   `member_ocean_isolation` policy (0009 §3a) — for an ordinary member
--   session that means the visible half of the subquery is already narrowed
--   to graphs that session can see at all, which is why the negative-write
--   test below can rely on `graphs`' own denial as a second, independent
--   backstop to the explicit ownership check.
--
-- Depends on: 0062_google_watch_channels.sql (precedent), 0009_tenant_rls.sql
--             (tenant_rls_visible_graph_ids()).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON microsoft_calendar_sync_state;
--   DROP TABLE IF EXISTS microsoft_calendar_sync_state;

CREATE TABLE IF NOT EXISTS microsoft_calendar_sync_state (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL,
    owner_user_id    UUID        NOT NULL,
    graph_id         UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    sync_enabled     BOOLEAN     NOT NULL DEFAULT TRUE,
    last_synced_at   TIMESTAMPTZ,
    last_error       TEXT,
    last_error_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);

ALTER TABLE microsoft_calendar_sync_state ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON microsoft_calendar_sync_state;
CREATE POLICY member_ocean_isolation ON microsoft_calendar_sync_state
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM graphs
            WHERE graphs.id = microsoft_calendar_sync_state.graph_id
              AND graphs.account_id = microsoft_calendar_sync_state.account_id
              AND graphs.owner_user_id = microsoft_calendar_sync_state.owner_user_id
              AND graphs.kind = 'member'
              AND graphs.archived_at IS NULL
        )
    );
