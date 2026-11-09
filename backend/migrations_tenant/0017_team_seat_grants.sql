-- Migration: 0017_team_seat_grants
-- Description: the TENANT half of control's `0106_team_seat_grants`, which
--   should have shipped paired and did not.
--
--   ── Why this is a correction ──
--   0106's header asserts that `teams`/`team_members` "live only in the
--   control tree (they are absent from `migrations_tenant/0001_tenant_
--   baseline.sql`)" and registers `team_seat_grants` in
--   `check_schema_parity.sh`'s CONTROL_TABLES on that basis. **The assertion
--   is false.** `0001_tenant_baseline.sql` creates `public.teams` (line 718)
--   and `public.team_members` (line 703, including `departing_at`), and
--   `0006_member_ocean_reachability_functions.sql` says so in its own header
--   — "`teams`/`team_members` are shared tenant-tree tables too" — then joins
--   them in `tenant_rls_visible_graph_ids()`. Team oceans are a live
--   tenant-plane feature.
--
--   The original check used `CREATE TABLE IF NOT EXISTS team_members`, which
--   the squashed baseline does not spell that way (`CREATE TABLE public.
--   team_members`), so it returned a false negative.
--
--   ── Why the parity guard did not catch it ──
--   It cannot. Listing a table in CONTROL_TABLES *instructs* the guard to
--   drop it from the comparison; the guard trusts that declaration. A table
--   wrongly declared control-only is invisible to the very check that exists
--   to find unpaired tables. The declaration is the assertion, and this
--   migration is the retraction.
--
--   ── Consequence this fixes ──
--   `team_members` exists tenant-side, `team_seat_grants` did not. Every
--   `crate::seats` call against a tenant pool — `record_seat_grant`,
--   `revoke_seat_grant`, `users_granted_by_source` — would have failed with
--   `relation "team_seat_grants" does not exist`. Latent today only because
--   connector authorization is still refused under org context
--   (`reject_org_oauth_management`), so no org account can hold a Slack
--   install to route. It is a direct blocker for org-plane Slack.
--
--   Shape is byte-equivalent to control's, minus the cross-plane FKs
--   (`accounts`, `users` are control-only) — the same convention
--   `0013_slack_watched_channels` follows, and what
--   `check_schema_parity.sh` expects once the table is compared rather than
--   dropped.
--
-- Depends on: 0001_tenant_baseline (teams, team_members),
--             0006_member_ocean_reachability_functions
--             (tenant_rls_visible_graph_ids)
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON team_seat_grants;
--   ALTER TABLE team_seat_grants DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS team_seat_grants;

CREATE TABLE IF NOT EXISTS team_seat_grants (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- No FK: accounts is control-only, does not exist in a tenant DB.
    account_id UUID        NOT NULL,
    team_id    UUID        NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    -- No FK: users is control-only, does not exist in a tenant DB.
    user_id    UUID        NOT NULL,
    -- 'slack:<channel_id>' or 'preexisting' — see the paired control
    -- migration's header for the full design. Must stay byte-equivalent to
    -- the control side's shape.
    source     TEXT        NOT NULL CHECK (source <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (team_id, user_id, source)
);

CREATE INDEX IF NOT EXISTS idx_team_seat_grants_account_source
    ON team_seat_grants (account_id, source);

ALTER TABLE team_seat_grants ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON team_seat_grants;
-- Anchored exactly like `team_members`' own tenant policy (0009, line 1391):
-- `team_id -> teams -> graph_id`, through the SECURITY DEFINER visibility
-- function. A grant is reachable precisely when the team ocean it seats
-- somebody into is reachable — anything looser would let a member enumerate
-- seat provenance for oceans they cannot see.
CREATE POLICY member_ocean_isolation ON team_seat_grants
    USING (team_id IN (
        SELECT id FROM teams
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));
