-- Migration 0074: gmail_everything_mode — tenant twin of
--   migrations_control/0197_gmail_everything_mode.sql (gsuite enterprise
--   train PR G2.4, "Gmail everything mode").
--
--   Byte-identical column set to the control-tree migration; see that
--   migration's header for the full column-by-column rationale, and
--   `migrations_tenant/0070_outlook_everything_mode.sql`'s own header (the
--   FIRST implementation of this carve-out's tenant-tree shape) for the
--   structural-difference conventions restated below.
--
--   * `account_id` — bare `UUID`, no `REFERENCES accounts(id)` (cross-DB
--     references stay bare uuid columns on this tree — `migrations_tenant/
--     0001_tenant_baseline.sql`'s own header).
--   * `owner_user_id` — bare `UUID`, no `REFERENCES users(id)`.
--   * `graph_id` — a REAL FK to `graphs(id)`, since `graphs` exists on the
--     tenant tree too.
--
--   RLS: `member_ocean_isolation` anchored on `graph_id` via
--   `tenant_rls_visible_graph_ids()` — same shape as `outlook_everything_
--   mode`'s own tenant-plane policy (0070), including its `WITH CHECK` (a
--   bare `USING`-only policy also governs writes, which on this table would
--   let a write pair a `graph_id` visible to the acting session with an
--   `owner_user_id` that is NOT that graph's real owner — see 0070's header
--   for the full ownership-subquery rationale).
--
--   No bootstrap/`SECURITY DEFINER` function on this tree: an org tenant
--   database holds exactly one account (ADR-0007), so `workers::
--   gmail_everything_mode`'s org-plane tick already knows its `account_id`
--   (resolved once from the control-plane `tenant_databases` row) and reads
--   this table directly inside a `begin_tenant_system_scoped` transaction —
--   no cross-account discovery problem to solve here, same reasoning 0070's
--   own header (and 0192's, for the control-only bootstrap it mirrors)
--   gives.
--
--   `claimed_until`/`claim_id` — same atomic-claim + fencing-token columns as
--   the control twin, taken by `workers::gmail_everything_mode::
--   enabled_owners_for_account`'s org-plane discovery query directly (an
--   `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING`, inline
--   in Rust rather than a `SECURITY DEFINER` function) — no bootstrap
--   function needed here for the same reason none is needed for discovery
--   itself: a single-account, already-scoped query has no cross-account
--   boundary for a bypass function to cross. See 0070's header for the full
--   incident history (F1/F2/F3) this shape closes.
--
--   `last_attempted_at` — NEW column, mirrors the control twin's own
--   addition; see 0197's header for the starvation incident this closes
--   (both failure paths now advance it, so a permanently-failing owner's
--   discovery sort key still moves forward and eventually rotates out).
--
--   `WITH CHECK` gates the graph-shape check on `NOT gmail_everything_mode.
--   enabled OR EXISTS (...)` — a disable UPDATE (which never touches
--   `graph_id`) always passes, account/owner-match aside. See 0197's
--   header, "WITH CHECK", for the full two-round incident (0070's Outlook
--   precedent has the IDENTICAL always-checked shape, unfixed there —
--   this PR's own responsibility is Gmail's copy only): leaving a member's
--   graph `enabled=true` after that graph is later archived (offboarding,
--   `member_ocean_reconciler`'s archive arm) made BOTH the owner's own
--   disable route AND `enabled_owners_for_account`'s per-tick claim UPDATE
--   fail RLS — the latter for the WHOLE claimed batch in one statement,
--   which on THIS tree means the entire org's tick, since one org holds
--   exactly one tenant DB (ADR-0007). Round 1 (drop `archived_at IS NULL`
--   alone) was reproduced insufficient by round 2 (any OTHER graph-shape
--   mismatch on a stale/orphaned row breaks disable identically) —
--   `NOT enabled OR` is the general fix both rounds converge on: a disable
--   needs no graph validation at all. Reproduced against real Postgres
--   under the adversarial review, 2026-08-25. No Personal-plane arm is
--   needed on this tree (unlike the control twin's own fix): every account
--   on the tenant tree is an Org account, and `graphs.kind='member'` is
--   already the only shape `resolve_org_member_primary_graph` ever
--   resolves here.
--
-- Depends on: 0070_outlook_everything_mode.sql (structural + mechanical
--             precedent), 0066_google_gmail_watch_owner_and_threads.sql
--             (Gmail's own per-member ownership precedent on this tree),
--             0009_tenant_rls.sql (tenant_rls_visible_graph_ids()).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON gmail_everything_mode;
--   DROP TABLE IF EXISTS gmail_everything_mode;

CREATE TABLE IF NOT EXISTS gmail_everything_mode (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL,
    owner_user_id    UUID        NOT NULL,
    graph_id         UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    enabled          BOOLEAN     NOT NULL DEFAULT FALSE,
    resume_cursor    TEXT,
    claimed_until    TIMESTAMPTZ,
    claim_id         UUID,
    last_synced_at   TIMESTAMPTZ,
    last_attempted_at TIMESTAMPTZ,
    last_error       TEXT,
    last_error_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);

ALTER TABLE gmail_everything_mode ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON gmail_everything_mode;
CREATE POLICY member_ocean_isolation ON gmail_everything_mode
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
    WITH CHECK (
        -- DISABLING skips the graph-shape check (round 2 of the
        -- adversarial review fix, superseding the control twin's own
        -- round-1 archived_at-only version) — see 0197's own header for the
        -- full incident, including the SECOND failure mode round 1 missed
        -- (a non-`archived_at` graph-shape mismatch on a stale/orphaned
        -- row also broke disable, not just archival).
        NOT gmail_everything_mode.enabled
        OR EXISTS (
            SELECT 1 FROM graphs
            WHERE graphs.id = gmail_everything_mode.graph_id
              AND graphs.account_id = gmail_everything_mode.account_id
              AND graphs.owner_user_id = gmail_everything_mode.owner_user_id
              AND graphs.kind = 'member'
        )
    );
