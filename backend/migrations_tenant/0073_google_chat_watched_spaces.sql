-- Migration 0073: google_chat_watched_spaces (tenant tree)
-- Description: Tenant-tree half of the paired
--   `migrations_control/0198_google_chat_watched_spaces.sql` — see that file
--   for the full rationale (the unrouted-by-default state D6 requires, the
--   archetype invariant, the two disjoint partial uniques, the columns
--   reserved for G4.4).
--
--   Byte-identical `CREATE TABLE` to the control side (per
--   `scripts/check_schema_parity.sh`'s diff), with the same cross-DB
--   structural differences `0066_google_gmail_watch_owner_and_threads.sql`
--   documents for `gmail_watched_threads` (`migrations_tenant/
--   0001_tenant_baseline.sql`'s convention: `accounts`/`users` are
--   control-plane-only, so cross-referencing columns are bare `uuid` here,
--   no FK):
--
--   * `account_id` — bare UUID, no `REFERENCES accounts(id)`.
--   * `owner_user_id` / `reader_user_id` — bare UUID, no `REFERENCES users(id)`.
--   * `graph_id` — a REAL FK to `graphs(id)`, since `graphs` exists on the
--     tenant tree too (same as every other tenant-tree destination table).
--
--   ── RLS: WITH CHECK on both planes from the start (design doc §1.8) ──
--   `gmail_watched_threads`' tenant-plane policy is `USING`-only (no
--   explicit `WITH CHECK`) — #692's round-1 finding, which this table does
--   NOT repeat: an explicit, NULL-friendly `WITH CHECK` ships here from the
--   start, matching the control side's own explicit re-assertion (see that
--   migration's header). Functionally this is what an omitted `WITH CHECK`
--   on a `FOR ALL` policy already defaults to (Postgres reuses `USING` when
--   `WITH CHECK` is not given) — writing it out explicitly is what the
--   design doc asks for so a future reader never has to reason about the
--   default, and so a future edit that tightens `USING` without touching
--   `WITH CHECK` (or vice versa) is a visible diff, not a silent divergence.
--
--   ── H2 fix (security review, this PR round 2): the `graph_id IS NULL`
--   arm needs an ACCOUNT/ACTOR anchor, unlike the control side's ──
--   On CONTROL, `graph_id IS NULL OR graph_id IN (...)` is safe because it
--   is conjoined with `account_id = current_setting('app.account_id')::uuid`
--   — an unrouted row is still gated by account. On TENANT there is no such
--   conjunct (one tenant DB IS one account, ADR-0007), so the ORIGINAL
--   `graph_id IS NULL OR graph_id IN (tenant_rls_visible_graph_ids())` was
--   an UNCONDITIONAL bypass for any unrouted row: `tenant_rls_visible_
--   graph_ids()` resolves per-MEMBER ocean visibility, not per-account, so
--   the NULL arm let ANY member session read/tamper with EVERY OTHER
--   member's unrouted rows (a Chat space's `display_name`, and — via
--   UPDATE — a denial-of-ingest primitive by flipping `active=false` on a
--   space that isn't theirs). Verified empirically against the real
--   `coralswarm_tenant_app` role under a member-scoped session: SELECT,
--   UPDATE, and a forged-`account_id` INSERT all succeeded before this fix.
--   Not exploitable TODAY (every write to this table runs
--   `begin_tenant_system_scoped`, never a member-scoped transaction — see
--   `connectors::google_chat`), but it would have gone live silently the
--   moment G4.5's opt-in route or G4.6's org lift added a member-scoped
--   caller. Fixed by gating the NULL arm on `tenant_rls_is_system()` (0009)
--   — the exact GUC `begin_tenant_system_scoped` sets and every current
--   caller already runs under, so this is a ZERO-behavior-change fix for
--   every code path that exists today, and a real fix for every one that
--   doesn't yet. An unrouted row is, definitionally, a system-level fact
--   (D6: the app's membership, independent of any one member) until a
--   destination pick assigns it a `graph_id` — a plain member session has
--   no legitimate reason to see one before that, so this is not a
--   regression waiting to be un-done once a member-scoped path DOES exist.
--
-- Depends on: 0001_tenant_baseline (accounts/users/graphs shapes),
--             0013_slack_watched_channels (the reference shape),
--             0009_tenant_rls (coralswarm_tenant_app role,
--             tenant_rls_visible_graph_ids()).
--
-- Paired with: migrations_control/0198_google_chat_watched_spaces.sql.
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON google_chat_watched_spaces;
--   ALTER TABLE google_chat_watched_spaces DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS google_chat_watched_spaces;

CREATE TABLE IF NOT EXISTS google_chat_watched_spaces (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- No FK: accounts is control-only, does not exist in a tenant DB.
    account_id         UUID        NOT NULL,
    space_id           TEXT        NOT NULL,
    space_type         TEXT        NOT NULL
                                    CHECK (space_type IN ('SPACE', 'GROUP_CHAT', 'DIRECT_MESSAGE')),
    mode               TEXT        CHECK (mode IS NULL OR mode IN ('single', 'fanout')),
    graph_id           UUID        REFERENCES graphs(id) ON DELETE CASCADE,
    -- No FK: users is control-only, does not exist in a tenant DB.
    owner_user_id      UUID,
    reader_user_id     UUID,
    display_name       TEXT        NOT NULL DEFAULT '',
    history_state      TEXT        NOT NULL DEFAULT '',
    active             BOOLEAN     NOT NULL DEFAULT TRUE,
    sync_token         TEXT,
    backfill_cursor    TEXT,
    backfill_complete  BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_resolved_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at   TIMESTAMPTZ,

    CONSTRAINT google_chat_watched_spaces_mode_graph_id_check CHECK (
        (mode = 'single' AND graph_id IS NOT NULL)
        OR (mode = 'fanout' AND graph_id IS NULL)
        OR (mode IS NULL AND graph_id IS NULL)
    ),
    CONSTRAINT google_chat_watched_spaces_archetype_check CHECK (
        (space_type = 'SPACE' AND owner_user_id IS NULL)
        OR (space_type <> 'SPACE' AND mode = 'single' AND owner_user_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_google_chat_watched_spaces_container
    ON google_chat_watched_spaces (account_id, space_id)
    WHERE owner_user_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_google_chat_watched_spaces_mailbox
    ON google_chat_watched_spaces (account_id, space_id, owner_user_id)
    WHERE owner_user_id IS NOT NULL;

ALTER TABLE google_chat_watched_spaces ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON google_chat_watched_spaces;
CREATE POLICY member_ocean_isolation ON google_chat_watched_spaces
    USING (
        (graph_id IS NULL AND tenant_rls_is_system())
        OR graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    )
    WITH CHECK (
        (graph_id IS NULL AND tenant_rls_is_system())
        OR graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    );
