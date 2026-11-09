-- Migration: 0184_team_seat_grants_history_from
-- Description: per-seat-grant retrieval window (m365 enterprise train PR
--   4.1). Adds `team_seat_grants.history_from` — NULL means full history,
--   a timestamp means "this grant only vouches for a seat that can read the
--   ocean from this instant forward".
--
--   ── Why a column on the grant, not on `team_members` ──
--   A seat can hold several grants at once (`0106`'s header: "the SAME
--   person seated by TWO routed channels"), and `docs/plans/
--   org-connectors-and-channel-ingest.md` §5 settles that a seat's effective
--   window is the WIDEST one any surviving grant justifies — a member seated
--   by a founding-history channel AND, independently, by a later `from_join`
--   channel can read from the beginning, because the founding grant alone
--   already earns that. That resolution (NULL absorbs; otherwise MIN) is a
--   property of the GRANT SET for a `(team_id, user_id)` pair, so it has to
--   be stored per grant, not once per seat — exactly the same "one row per
--   reason" argument 0106's header already made for the table's existence.
--
--   ── Byte-equivalent to the tenant twin ──
--   The paired tenant migration is `migrations_tenant/
--   0064_team_seat_grants_history_from.sql`, per the `0106`/`0017`
--   byte-equivalent-shape convention (see that pair's headers). This side
--   adds only the column — the tenant side additionally carries the RLS
--   enforcement (the new `tenant_rls_visible_graph_windows()` function and
--   the `atoms` policy rework), because `team_seat_grants`/`team_members`/
--   `atoms` RLS all live on the tenant tree only (control's `team_seat_grants`
--   here is governed by 0106's `account_isolation` policy, which needs no
--   change — a nullable column addition does not touch that predicate).
--
--   ── Column semantics ──
--   NULL = full history (the founding-member / auto-seated-by-routing
--   default, §4/§5 of the plan doc). A timestamp = "atoms whose source-event
--   time is before this are outside this grant's window" — enforced at
--   retrieval time on the TENANT plane only; this column exists on control
--   purely for schema-parity (control's `team_seat_grants` rows are never
--   read by a retrieval path — the control tree has no `atoms` table content
--   query to window).
--
--   ── `seat_grant_sources` (CodeRabbit round 1 on PR #680) ──
--   The reconcilers' founding-sweep rule ("no grants exist yet for this
--   `(team_id, source)` ⇒ this pass seats the founding roster, full
--   history") originally read ONLY surviving `team_seat_grants` rows — but
--   `revoke_seat_grant` DELETEs rows, so a source whose every member left
--   would look brand-new to the next sweep, and the next person to join a
--   long-routed channel would be mis-stamped a founding member (full ocean
--   history). This table is the durable founding marker: one row per
--   `(team_id, source)`, written the first time a sweep (or the routing-time
--   auto-seat) actually seats someone for that source, NEVER deleted by
--   grant revocation. Keyed by team, not by watched-channel row, because a
--   channel re-routed to a DIFFERENT team founds that team's grants afresh
--   (same rationale as the grant table's own (team_id, ..., source) key).
--
-- Depends on: 0106_team_seat_grants, 0022_teams
-- Down:
--   DROP TABLE IF EXISTS seat_grant_sources;
--   ALTER TABLE team_seat_grants DROP COLUMN IF EXISTS history_from;

ALTER TABLE team_seat_grants ADD COLUMN IF NOT EXISTS history_from TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS seat_grant_sources (
    account_id  UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    team_id     UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    source      TEXT NOT NULL CHECK (source <> ''),
    founded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (team_id, source)
);

CREATE INDEX IF NOT EXISTS idx_seat_grant_sources_account
    ON seat_grant_sources (account_id);

-- Same control-plane policy shape as 0106's team_seat_grants: account
-- isolation, with the write side additionally pinning team ownership.
ALTER TABLE seat_grant_sources ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON seat_grant_sources;
CREATE POLICY account_isolation ON seat_grant_sources
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND team_id IN (
            SELECT id FROM teams
            WHERE account_id = current_setting('app.account_id')::uuid
        )
    );
