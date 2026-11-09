-- Migration 0070: outlook_everything_mode — tenant twin of
--   migrations_control/0192_outlook_everything_mode.sql (m365 enterprise
--   train PR 3.4, "Outlook everything mode").
--
--   Byte-identical column set to the control-tree migration; see that
--   migration's header for the full column-by-column rationale (`enabled
--   DEFAULT FALSE` = the opt-in "never default" requirement; `resume_cursor`
--   = the resumable whole-mailbox pagination cursor). Structural differences
--   required by the tenant-tree convention (`migrations_tenant/
--   0001_tenant_baseline.sql`'s own header: cross-DB references are bare
--   uuid columns with no FK — users/accounts live in the control DB only),
--   same split `microsoft_calendar_sync_state` (0067) already uses:
--
--   * `account_id` — bare `UUID`, no `REFERENCES accounts(id)`.
--   * `owner_user_id` — bare `UUID`, no `REFERENCES users(id)`.
--   * `graph_id` — a REAL FK to `graphs(id)`, since `graphs` exists on the
--     tenant tree too.
--
--   RLS: `member_ocean_isolation` anchored on `graph_id` via
--   `tenant_rls_visible_graph_ids()` — same shape as `microsoft_calendar_
--   sync_state`'s own tenant-plane policy (0067), including its `WITH CHECK`
--   (CodeRabbit round 1 on PR #692's finding, closed there and applied here
--   from the start rather than retrofitted): a bare `USING`-only policy also
--   governs writes, which on this table would let a write pair a `graph_id`
--   visible to the acting session with an `owner_user_id` that is NOT that
--   graph's real owner. The `graphs`-derived `WITH CHECK` below closes that,
--   identically in shape to 0067's — see that migration's header for the
--   full ownership-subquery rationale (only the tenant-tree specifics —
--   `account_id` compared directly rather than via a control-only GUC — are
--   restated here).
--
--   No bootstrap/`SECURITY DEFINER` function on this tree: an org tenant
--   database holds exactly one account (ADR-0007), so
--   `workers::outlook_everything_mode`'s org-plane tick already knows its
--   `account_id` (resolved once from the control-plane `tenant_databases`
--   row, the same `resolve_tenant_account` pattern `workers::
--   microsoft_calendar_sync`/`workers::graph_subscriptions` already use) and
--   reads this table directly inside a `begin_tenant_system_scoped`
--   transaction — no cross-account discovery problem to solve here, same
--   reasoning 0192's own header (and 0181's, for the control-only bootstrap
--   it mirrors) gives.
--
--   `claimed_until` (CodeRabbit round 2 on PR #700, F1) — same atomic-claim
--   column as the control twin; see 0192's own header for the full
--   incident (two processes racing the same due owner can clobber a NEWER
--   `resume_cursor` with a stale one). On THIS tree the claim is taken by
--   `workers::outlook_everything_mode::enabled_owners_for_account`'s org-
--   plane discovery query directly (an `UPDATE ... FROM (SELECT ... FOR
--   UPDATE SKIP LOCKED) RETURNING`, inline in Rust rather than a `SECURITY
--   DEFINER` function) — no bootstrap function needed here for the SAME
--   reason none is needed for discovery itself: a single-account, already-
--   scoped query has no cross-account boundary for a bypass function to
--   cross. `record_progress`/`mark_failed_no_progress` release-on-success /
--   back-off-on-failure it the same way on both trees (CodeRabbit round 3,
--   F2, corrected from round 2's unconditional `NULL` release on failure —
--   see 0192's own header for the full "permanently-failing owner
--   monopolizes every batch" incident and why a 30-minute `claimed_until`
--   backoff, not a `last_synced_at`-touching reorder, is the fix).
--
--   `claim_id` (CodeRabbit round 4 on PR #700, F3) — same fencing-token
--   column as the control twin; see 0192's own header, "claim_id", for the
--   full stale-worker-overwrite incident this closes. On THIS tree the
--   token is stamped by `enabled_owners_for_account`'s claiming `UPDATE`
--   directly (`claim_id = gen_random_uuid()` in the same statement that
--   sets `claimed_until`), not a `SECURITY DEFINER` function — same
--   reasoning as `claimed_until` above, no cross-account boundary for a
--   bypass function to cross. `record_progress`/`mark_failed_no_progress`
--   gate their completion/failure writes on `AND claim_id = $n` identically
--   on both trees.
--
-- Depends on: 0067_microsoft_calendar_sync_state.sql (structural precedent),
--             0009_tenant_rls.sql (tenant_rls_visible_graph_ids()).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON outlook_everything_mode;
--   DROP TABLE IF EXISTS outlook_everything_mode;

CREATE TABLE IF NOT EXISTS outlook_everything_mode (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL,
    owner_user_id    UUID        NOT NULL,
    graph_id         UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    enabled          BOOLEAN     NOT NULL DEFAULT FALSE,
    resume_cursor    TEXT,
    claimed_until    TIMESTAMPTZ,
    claim_id         UUID,
    last_synced_at   TIMESTAMPTZ,
    last_error       TEXT,
    last_error_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);

ALTER TABLE outlook_everything_mode ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON outlook_everything_mode;
CREATE POLICY member_ocean_isolation ON outlook_everything_mode
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM graphs
            WHERE graphs.id = outlook_everything_mode.graph_id
              AND graphs.account_id = outlook_everything_mode.account_id
              AND graphs.owner_user_id = outlook_everything_mode.owner_user_id
              AND graphs.kind = 'member'
              AND graphs.archived_at IS NULL
        )
    );
