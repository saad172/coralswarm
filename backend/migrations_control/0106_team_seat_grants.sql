-- Migration: 0106_team_seat_grants
-- Description: provenance for auto-granted team seats, so a seat can be
--   revoked when the reason for it goes away — and, just as importantly, so
--   a seat that was NOT auto-granted can never be revoked by mistake.
--
--   ── Why a table and not a column ──
--   Auto-seating (docs/connectors/slack.md's C5 row, founder decision
--   bd17b10 / PR #372) grants a team-ocean seat because a Slack channel was
--   routed into that ocean. The obvious cheap shape — one `granted_by_slack`
--   boolean on `team_members` — cannot express the case that actually
--   matters: the SAME person seated by TWO routed channels. Leaving one of
--   those channels must NOT cost them the seat the other channel still
--   justifies. A boolean forces a choice between revoking too eagerly
--   (losing access someone still has a live reason for) and never revoking
--   at all (the status quo, where an auto-granted seat is permanent).
--
--   So: one row per REASON. `(team_id, user_id, source)`. The seat lives
--   while any row for that `(team_id, user_id)` lives.
--
--   ── `source` values ──
--   * `slack:<channel_id>` — this channel is routed into this team's ocean
--     and this person is in it. Written by auto-seating and by the
--     `member_joined_channel` handler; deleted by `member_left_channel` and
--     by the reconciler when a roster walk no longer finds them.
--   * `preexisting` — recorded when auto-seating found the person ALREADY
--     holding an active seat it did not create. This row is never deleted
--     by any Slack event, which is precisely what makes it load-bearing:
--     see the revocation invariant below.
--
--   ── The revocation invariant ──
--   Revocation may ONLY ever fire as a consequence of deleting a grant row
--   that we ourselves recorded. Concretely, `revoke_seat_grant` refuses to
--   touch `team_members` unless its DELETE actually removed a row. Without
--   that rule, a seat predating this table (no grants at all) would read as
--   "zero grants remain" and be revoked on the first `member_left_channel`
--   for an unrelated channel — silently removing access a human granted by
--   hand. Absence of provenance means "not ours to revoke", never "safe to
--   revoke".
--
--   The `preexisting` row extends the same protection to a seat that was
--   manual FIRST and auto-granted SECOND: its grant set never empties, so
--   the revocation branch is unreachable for that pair.
--
--   Admins are additionally protected in code (`role <> 'admin'`) —
--   auto-seating never grants 'admin', so it must never take one away.
--
--   ── Control-only ──
--   `teams`/`team_members` live only in the control tree (they are absent
--   from `migrations_tenant/0001_tenant_baseline.sql`), so this table is
--   control-only too and is registered in `scripts/check_schema_parity.sh`'s
--   CONTROL_TABLES list in this same PR, per that script's own instruction.
--   There is deliberately no paired tenant migration.
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON team_seat_grants;
--   ALTER TABLE team_seat_grants DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS team_seat_grants;

CREATE TABLE IF NOT EXISTS team_seat_grants (
    id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    team_id    UUID        NOT NULL REFERENCES teams(id)    ON DELETE CASCADE,
    user_id    UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    -- 'slack:<channel_id>' or 'preexisting' — see this migration's header.
    -- Deliberately TEXT rather than an enum: a future connector granting
    -- seats for its own reason ('teams:<channel>', say) must be able to add
    -- a source without a migration, and the revocation logic only ever cares
    -- about EXACT-matching the source it is deleting.
    source     TEXT        NOT NULL CHECK (source <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One grant per reason. The ON CONFLICT target for the idempotent
    -- re-grant on every roster walk, and the composite index the revocation
    -- lookup (`WHERE team_id = $1 AND user_id = $2 AND source = $3`) and the
    -- remaining-grant count (`WHERE team_id = $1 AND user_id = $2`) both
    -- read through — no separate CREATE INDEX needed for either.
    UNIQUE (team_id, user_id, source)
);

-- The reconciler's per-channel sweep shape: "every grant this channel is
-- responsible for", to diff against a fresh roster walk. Not covered by the
-- UNIQUE above (which leads with team_id).
CREATE INDEX IF NOT EXISTS idx_team_seat_grants_account_source
    ON team_seat_grants (account_id, source);

ALTER TABLE team_seat_grants ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON team_seat_grants;
-- READ: `account_id` alone is a correct, direct, unambiguous anchor.
-- WRITE: additionally requires `team_id` to belong to a team owned by that
-- SAME account. Same cross-account-pointer class as 0083's `node_promotions`
-- and 0101's `slack_watched_channels`: a bare USING reused as WITH CHECK
-- would let a caller scoped to account A insert a grant naming account B's
-- team, and this table's whole purpose is deciding who keeps a seat.
CREATE POLICY account_isolation ON team_seat_grants
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND team_id IN (
            SELECT id FROM teams WHERE account_id = current_setting('app.account_id')::uuid
        )
    );
