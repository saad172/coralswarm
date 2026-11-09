-- Migration: 0404_journal_gists
-- PAIRED WITH: tenant 0218 (journal_gists). Same table, index, function and
-- trigger; this control twin adds the `accounts` foreign key (control-only
-- table) and anchors RLS on `account_id`, the 0024/0113 convention.
--
-- What. `journal_gists`: the Today workstream panel's gist (3-5 terse
-- DONE / DECIDED / OPEN bullets) for one day, week or month, compiled by
-- `services::journal_gist` inside the existing `journal_sweep` worker and read
-- by `GET /v1/journal/gist` (flag TODAY_GIST_ENABLED). Contract:
-- docs/implementation/journal-gist-api.md.
--
-- Why a new table, not a `gist` column on `journal_rollups`.
--   1. Team. A team gist belongs to an OCEAN, not to a member.
--      `journal_rollups` is keyed on a NOT NULL `user_id`; making it nullable
--      for one scope would change the natural key, the fingerprint discovery
--      and every reader of a table several workers and routes depend on.
--   2. The open period. The Day gist leads Today while the day is still
--      running. `journal_rollups` deliberately has no row for an open period
--      (it is composed live), so a column there would have nowhere to live
--      until midnight.
--   3. Deletion follows for free: the statement trigger below drops every
--      gist overlapping a deleted rollup, so each retraction path that already
--      deletes rollups (atom purge, access restriction, the stale-input sweep,
--      account deletion) retracts the gist too, and the next sweep recompiles
--      it from what is left.
--
-- Scope. `member` rows belong to one member in one ocean (`user_id` set);
-- `team` rows belong to the ocean (`user_id` NULL). The CHECK ties the two.
-- One row per (member, ocean, granularity, period start) or per (ocean,
-- granularity, period start): two partial unique indexes, because one UNIQUE
-- over a nullable `user_id` would treat every team row as distinct.
--
-- `period_end` is stored (not derived) so the read path can find the row
-- whose period contains the requested anchor even when the reader's zone
-- differs from the zone the row was compiled in (`tz`).
--
-- Additive only: a new table, index, function and trigger.
--
-- Down:
--   DROP TRIGGER IF EXISTS journal_rollups_retract_gists ON journal_rollups;
--   DROP FUNCTION IF EXISTS journal_gists_follow_rollup_delete();
--   DROP TABLE IF EXISTS journal_gists;

CREATE TABLE IF NOT EXISTS journal_gists (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- FK here only: accounts is control-only (the tenant twin has none).
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    graph_id          UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    scope             TEXT        NOT NULL CHECK (scope IN ('member', 'team')),
    user_id           UUID,
    granularity       TEXT        NOT NULL CHECK (granularity IN ('day', 'week', 'month')),
    period_start      TIMESTAMPTZ NOT NULL,
    period_end        TIMESTAMPTZ NOT NULL,
    tz                TEXT        NOT NULL CHECK (tz <> ''),
    -- {"done": [..], "decided": [..], "open": [..]}, validated and trimmed
    -- server-side (at most 5 bullets in total, each under 60 characters).
    -- NULL on a row that has only failed attempts so far (see below); the
    -- read path never serves such a row.
    gist              JSONB,
    -- child_count, decision_count, member_count: read-only metadata.
    stats             JSONB       NOT NULL DEFAULT '{}',
    -- sha256 of the exact prompt of the last SUCCESSFUL compile (inputs and
    -- prompt version). An equal fingerprint means there is nothing to
    -- recompile. NULL while `gist` is NULL.
    input_fingerprint TEXT        CHECK (input_fingerprint <> ''),
    recompile_count   SMALLINT    NOT NULL DEFAULT 0,
    model             TEXT        CHECK (model <> ''),
    compiled_at       TIMESTAMPTZ DEFAULT now(),
    -- Attempt bookkeeping, so a failing period (an unusable answer, inputs
    -- that moved, a write error) backs off instead of being retried every
    -- sweep pass: the last attempt, its error, the consecutive failures since
    -- the last success, and the calls this row spent on `attempt_day` (UTC),
    -- which the plane-wide daily cap sums.
    last_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error        TEXT,
    failed_attempts   SMALLINT    NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
    attempt_day       DATE        NOT NULL DEFAULT ((now() AT TIME ZONE 'UTC')::date),
    attempt_day_calls INTEGER     NOT NULL DEFAULT 0 CHECK (attempt_day_calls >= 0),
    CONSTRAINT journal_gists_scope_user CHECK ((scope = 'member') = (user_id IS NOT NULL)),
    CONSTRAINT journal_gists_period CHECK (period_end > period_start),
    CONSTRAINT journal_gists_compiled CHECK (
        (gist IS NULL AND compiled_at IS NULL AND input_fingerprint IS NULL AND model IS NULL)
        OR (gist IS NOT NULL AND compiled_at IS NOT NULL AND input_fingerprint IS NOT NULL AND model IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS journal_gists_member_key
    ON journal_gists (user_id, graph_id, granularity, period_start) WHERE scope = 'member';
CREATE UNIQUE INDEX IF NOT EXISTS journal_gists_team_key
    ON journal_gists (graph_id, granularity, period_start) WHERE scope = 'team';
-- The read: one granularity, the reader's oceans, the period containing the
-- anchor.
CREATE INDEX IF NOT EXISTS journal_gists_read_idx
    ON journal_gists (graph_id, granularity, period_start);

-- A deleted rollup retracts every gist that could have read it: the same
-- member's gists in that ocean and the ocean's team gists, for any period
-- overlapping the rollup's. The rollup's end is over-approximated (a day row
-- may be 23 or 25 hours long); an extra deletion only costs one recompile.
-- SECURITY DEFINER so a member-scoped delete (RLS) also reaches the team rows
-- of the same ocean.
CREATE OR REPLACE FUNCTION journal_gists_follow_rollup_delete() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    DELETE FROM public.journal_gists g
     USING (SELECT DISTINCT o.graph_id, o.user_id, o.period_start,
                   o.period_start + CASE o.granularity
                       WHEN 'hour' THEN interval '1 hour'
                       WHEN 'day' THEN interval '26 hours'
                       WHEN 'week' THEN interval '8 days'
                       ELSE interval '32 days' END AS period_end
              FROM gone o) r
     WHERE g.graph_id = r.graph_id
       AND (g.scope = 'team' OR g.user_id = r.user_id)
       AND g.period_start < r.period_end
       AND g.period_end > r.period_start;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS journal_rollups_retract_gists ON journal_rollups;
CREATE TRIGGER journal_rollups_retract_gists
    AFTER DELETE ON journal_rollups
    REFERENCING OLD TABLE AS gone
    FOR EACH STATEMENT EXECUTE FUNCTION journal_gists_follow_rollup_delete();

ALTER TABLE journal_gists ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON journal_gists;
CREATE POLICY account_isolation ON journal_gists
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
