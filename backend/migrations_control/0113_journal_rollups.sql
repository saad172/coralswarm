-- Migration: 0113_journal_rollups
-- Description: `journal_rollups` — one row per (user, ocean, hour) holding an
--   LLM-compiled 1-3 sentence prose summary of that hour's activity. This is
--   the storage foundation for the rebuilt `/today` work journal, split
--   across four PRs:
--     PR A (this one)  — this migration + the `Store` read/write layer.
--     PR B             — a background sweep that finds "dirty" hours
--                         (`Store::journal_dirty_hours`) and compiles them
--                         via an LLM into rows here.
--     PR C             — the `GET` endpoint that reads rollups for a date
--                         range (`Store::list_journal_rollups`).
--     PR D             — the `/today` web UI.
--   No worker and no endpoint land in this PR — see `Store::journal_dirty_hours`
--   / `Store::journal_hour_inputs` for the exact contract PR B is expected to
--   drive off of.
--
-- PAIRED WITH: migrations_tenant/0024_journal_rollups.sql — same table shape
--   (`scripts/check_schema_parity.sh`), minus the `accounts`/`users` FKs that
--   don't exist on the tenant plane (the `0106_team_seat_grants` /
--   `0017_team_seat_grants` convention — see that pair's headers). RLS is
--   NOT shared: this file's `account_isolation` predicate is
--   control-plane-only, the tenant file's `member_ocean_isolation` predicate
--   is tenant-plane-only (same split as every other 0083/0009-covered table).
--
-- Depends on: 0003_atoms (atoms, the compile input this table is derived
--   from), 0013_activity_feed (activity_events, the other compile input),
--   0083_consumer_rls (the `account_isolation` RLS pattern this follows for
--   a direct-`account_id` content table).
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON journal_rollups;
--   ALTER TABLE journal_rollups DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS journal_rollups;
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY ONE ROW PER (user, graph, HOUR), NOT PER DAY OR PER SESSION
-- ─────────────────────────────────────────────────────────────────────────────
--
-- An hour is the smallest unit that is both (a) cheap enough to recompile in
-- full whenever its inputs change — no partial-hour patching, no diffing
-- prose against prose — and (b) coarse enough that a single LLM call
-- (PR B) produces a genuinely readable 1-3 sentence summary instead of one
-- sentence per atom. `hour_start` is always the UTC hour boundary
-- (`date_trunc('hour', ...)` on the input atoms' `timestamp`, computed the
-- same way by `Store::journal_dirty_hours` and every `Store::upsert_journal_rollup`
-- caller — see `store::journal_input_fingerprint`'s own doc comment for why
-- that arithmetic lives in exactly one place). `user_id` scopes a journal to
-- one person's own hour — this is a personal work log, not an account-wide
-- feed — and `graph_id` keeps a multi-ocean account's rollups separable per
-- ocean rather than force-merging every ocean's hour into one summary.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY A FINGERPRINT COLUMN, NOT A "dirty" FLAG OR A TIMESTAMP COMPARISON
-- ─────────────────────────────────────────────────────────────────────────────
--
-- The sweep (PR B) needs to answer "has anything about this hour changed
-- since I last compiled it?" without re-reading and re-diffing every input
-- atom's content on every pass. A plain `MAX(created_at)` comparison is
-- insufficient on its own: an atom can be DELETED (dropping the count
-- without moving the max), so `input_fingerprint` folds in `input_count`
-- alongside `MAX(created_at)`/`MAX(id)` specifically to catch that case. The
-- fingerprint is a plain `TEXT` rather than e.g. a `bytea` digest because it
-- is only ever compared for equality, never manipulated, and the codebase's
-- md5-hex-string convention elsewhere (`content_blob_key`s, etc.) makes a
-- 32-char hex string the least surprising shape here too.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `recompile_count` INSTEAD OF JUST OVERWRITING SILENTLY
-- ─────────────────────────────────────────────────────────────────────────────
--
-- A recompiled hour has strictly MORE input than the version it replaces
-- (new atoms/events landed after the first compile) — the counter is a
-- cheap, queryable signal of "this hour's story kept growing after its first
-- pass," useful for the eventual UI (a "updated" badge) and for debugging a
-- sweep that is recompiling the same hour far more often than expected
-- (e.g. a fingerprint bug that never stabilizes). `SMALLINT` is generous
-- headroom (32767) for a counter that increments at most once per sweep
-- cadence per hour.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `UNIQUE (user_id, graph_id, hour_start)` AND NOT A COMPOSITE PK
-- ─────────────────────────────────────────────────────────────────────────────
--
-- A separate `id UUID PRIMARY KEY` (rather than the natural key itself)
-- follows this codebase's own convention for every content table with a
-- natural uniqueness constraint (`atoms`, `nodes`, etc.) — a stable
-- surrogate id is simpler for a future FK (e.g. a citation pointing at one
-- rollup) than a 3-column composite key would be. `UNIQUE (user_id,
-- graph_id, hour_start)` is the `ON CONFLICT` target
-- `PostgresStore::upsert_journal_rollup` writes against.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- RLS
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Direct-`account_id`-column content table, so this follows 0083's exact
-- class-(a) predicate (see that migration's own header): the backstop that
-- makes an unscoped `journal_rollups` read hard-error under `coralswarm_app`
-- rather than silently leak. Reads are ADDITIONALLY scoped to `user_id` at
-- the application layer (a journal is per-member, exactly like
-- `activity_events`' own `user_id = caller_user_id OR user_id IS NULL`
-- app-level filter in `Store::list_activity`/`list_activity_scoped`) —
-- `account_id` is the RLS backstop's boundary, `user_id` is the product
-- boundary, and the two are deliberately independent layers, same split as
-- `activity_events`.

CREATE TABLE IF NOT EXISTS journal_rollups (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id           UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    graph_id          UUID        NOT NULL REFERENCES graphs(id)   ON DELETE CASCADE,
    -- Always a UTC hour boundary (date_trunc('hour', ...) on the compiled
    -- inputs' timestamps) — see this migration's header and
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
    -- is implemented, and this migration's header for why a fingerprint
    -- rather than a timestamp comparison.
    input_fingerprint TEXT        NOT NULL CHECK (input_fingerprint <> ''),
    -- 0 on first compile; incremented on every subsequent recompile (see
    -- header). Never decremented.
    recompile_count   SMALLINT    NOT NULL DEFAULT 0,
    -- The LLM model id that produced `summary`, for audit/debugging.
    model             TEXT        NOT NULL CHECK (model <> ''),
    compiled_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (user_id, graph_id, hour_start)
);

-- Primary read query (PR C): a user's rollups across one or more oceans,
-- newest hour first, within an RLS-scoped account.
CREATE INDEX IF NOT EXISTS journal_rollups_read_idx
    ON journal_rollups (account_id, user_id, hour_start DESC);

ALTER TABLE journal_rollups ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON journal_rollups;
CREATE POLICY account_isolation ON journal_rollups
    USING (account_id = current_setting('app.account_id')::uuid);
