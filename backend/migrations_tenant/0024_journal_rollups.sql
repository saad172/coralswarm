-- Migration: 0024_journal_rollups
-- Description: `journal_rollups` — the TENANT half of control's
--   `0113_journal_rollups`. See that migration's header for the full design
--   rationale (why per-hour, why a fingerprint column, why
--   `recompile_count`, the PR A/B/C/D roadmap) — not repeated here to avoid
--   the two copies drifting out of sync in prose while the DDL itself is
--   kept byte-equivalent.
--
-- PAIRED WITH: migrations_control/0113_journal_rollups.sql — same table
--   shape, minus the `accounts`/`users` FKs that don't exist on the tenant
--   plane (the `0106_team_seat_grants` / `0017_team_seat_grants` convention
--   — see that pair's headers for the general rule).
--
-- Depends on: 0001_tenant_baseline (atoms, activity_events, graphs),
--   0009_tenant_rls (the `member_ocean_isolation` RLS pattern this follows
--   for a direct-`graph_id` content table).
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON journal_rollups;
--   ALTER TABLE journal_rollups DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS journal_rollups;
--
-- RLS here is the TENANT-plane mirror of 0113's `account_isolation` policy —
-- same `activity_events`/`atoms` precedent (0009's own section for every
-- direct-`graph_id` table): `journal_rollups.graph_id` is a real column, so
-- the policy anchors directly on it through the SECURITY DEFINER
-- reachability function, no join needed.

CREATE TABLE IF NOT EXISTS journal_rollups (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- No FK: accounts is control-only, does not exist in a tenant DB.
    account_id        UUID        NOT NULL,
    -- No FK: users is control-only, does not exist in a tenant DB.
    user_id           UUID        NOT NULL,
    graph_id          UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    -- Always a UTC hour boundary (date_trunc('hour', ...) on the compiled
    -- inputs' timestamps) — see 0113's header and
    -- `store::journal_input_fingerprint`.
    hour_start        TIMESTAMPTZ NOT NULL,
    -- 1-3 sentence LLM prose for this hour (PR B). Never empty.
    summary           TEXT        NOT NULL CHECK (summary <> ''),
    -- atom_count, per-source-type counts, ask/interaction/system-event
    -- counts, etc. — free-form, read-only metadata never gated on at query
    -- time; see PR B for the exact shape it writes.
    stats             JSONB       NOT NULL DEFAULT '{}',
    -- md5(input_count || max(created_at) || max(id)) over the hour's atoms —
    -- see `store::journal_input_fingerprint`, the one place this arithmetic
    -- is implemented, and 0113's header for why a fingerprint rather than a
    -- timestamp comparison.
    input_fingerprint TEXT        NOT NULL CHECK (input_fingerprint <> ''),
    -- 0 on first compile; incremented on every subsequent recompile (see
    -- 0113's header). Never decremented.
    recompile_count   SMALLINT    NOT NULL DEFAULT 0,
    -- The LLM model id that produced `summary`, for audit/debugging.
    model             TEXT        NOT NULL CHECK (model <> ''),
    compiled_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (user_id, graph_id, hour_start)
);

-- Primary read query (PR C): a user's rollups across one or more oceans,
-- newest hour first, within a reachable-graph-scoped tenant connection.
CREATE INDEX IF NOT EXISTS journal_rollups_read_idx
    ON journal_rollups (account_id, user_id, hour_start DESC);

ALTER TABLE journal_rollups ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON journal_rollups;
CREATE POLICY member_ocean_isolation ON journal_rollups
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
