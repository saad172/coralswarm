-- Migration: 0095_platform_metrics
-- Description: PR 1 of 7 of the platform-admin-metrics feature
--              (docs/plans/platform-admin-metrics.md, if/when that plan doc
--              lands — this PR is schema + purge worker only, no routes, no
--              middleware). Three CONTROL-ONLY tables: no tenant-tree
--              counterpart exists for any of them (and none should ever be
--              added — see the "CI note" at the bottom of this comment,
--              mirroring 0092_member_ocean_control_tables's own note).
--
--              ── user_access_log ──
--              One row per PREVIOUSLY-UNSEEN Clerk session id, i.e. one row
--              per login (not per request). The middleware that will write
--              this table (a later PR in this series — out of scope here)
--              is expected to `INSERT ... ON CONFLICT (user_id,
--              clerk_session_id) DO NOTHING` on every authenticated request;
--              the UNIQUE constraint on `(user_id, clerk_session_id)` is what
--              makes that idempotent — the first request of a session
--              inserts a row, every subsequent request on the same session
--              is a no-op conflict, and "one row per login" falls out of the
--              constraint rather than needing to be enforced in application
--              code.
--
--              `client` is nullable: older desktop/web builds that predate
--              whatever header or payload field eventually carries a client
--              identifier will simply send nothing, and that is a normal,
--              expected value here, not a data-quality problem.
--
--              `ip`/`country`/`region`/`city` are raw geolocation inputs
--              (`ip` literally an `INET`, the others resolved from it by
--              whatever the future middleware wires up). Same category of
--              sensitive-but-necessary data as `mcp_tool_calls`
--              (0061_mcp_tool_calls.sql) tries hard to AVOID storing (that
--              table's whole design is "never store the thing that would
--              need special retention handling") — this table is the
--              opposite case: raw IP is exactly the signal a login/access
--              audit trail needs, so it is stored, but that is precisely why
--              it carries its own explicit, shorter retention policy instead
--              of inheriting an audit table's usual "keep forever" posture.
--
--              *** RETENTION: 30 DAYS, RAW IP DATA. *** Every row in this
--              table holds a raw IP address, which is personal data in most
--              privacy regimes. `backend/src/workers/access_log_purge.rs`
--              (this same PR) deletes rows older than
--              `ACCESS_LOG_RETENTION_DAYS` (30) on a daily sweep — mirroring
--              `workers::tombstone_purge`'s cadence and shape, see that
--              worker's own module doc for the pattern this one is modelled
--              on. Nothing downstream of this table should assume rows
--              persist longer than that window; anything that needs a
--              longer-lived signal derived from logins (e.g. a rollup count)
--              belongs in `platform_metrics_daily`/`platform_active_users`
--              below, not here.
--
--              Not `mcp_tool_calls` (that is a tool-call audit log scoped to
--              the MCP surface, deliberately content-free by construction)
--              and not `activity_events` (CHECK-constrained, product-facing
--              feed) — this is an operational login/session log with its
--              own 30-day retention, closest in shape to `mcp_tool_calls`
--              but opposite in what it is willing to store (raw IP, not a
--              digest) precisely because its retention window is short
--              enough to make that an acceptable trade.
--
--              Indexes: `(created_at)` is the purge worker's own access
--              pattern (delete everything older than the cutoff); `(user_id,
--              created_at DESC)` is the "this user's login history, newest
--              first" operator/analytics access pattern. The UNIQUE
--              constraint on `(user_id, clerk_session_id)` already provides
--              an index that would satisfy point lookups by
--              `clerk_session_id` alone if ever needed, so no separate index
--              is added for that column.
--
--              ── platform_metrics_daily ──
--              The cross-tenant rollup: one row per (day, tenant). `tenant`
--              is the literal string `'__consumer__'` for the personal/
--              consumer plane (there is no `clerk_org_id` to key on there —
--              every other tenant row uses its real `clerk_org_id`). This is
--              a plain TEXT discriminator, not an enum or a foreign key: it
--              has to represent both "the one shared consumer database" and
--              "any one of N per-org tenant databases" uniformly, and there
--              is no single table in the control DB whose primary key
--              spans both of those.
--
--              Deliberately NOT stored here: user/org/waitlist GROWTH
--              curves. Those are cumulative counts over time (e.g. "org
--              signups per day"), and every one of them is derivable on
--              demand, exactly and retroactively, from a `created_at`
--              column that already exists on the source-of-truth table
--              (`users.created_at`, `accounts.created_at`, whatever the
--              waitlist table's equivalent is) — `SELECT date_trunc('day',
--              created_at), count(*) FROM ... GROUP BY 1` reconstructs the
--              exact historical curve at query time, including any day in
--              the past, with no rollup table to keep in sync and no risk
--              of a rollup silently drifting from the source rows it was
--              computed from (a backfill, an undo, a manual data fix — any
--              of those would leave a materialized snapshot wrong until the
--              next recompute, whereas a live query is never wrong). This
--              table exists ONLY for numbers that genuinely cannot be
--              recomputed after the fact from a durable `created_at`-style
--              column — atom/reef/session counts and LLM token usage are
--              per-day AGGREGATE FACTS about activity, not timestamped
--              individual rows that a `GROUP BY created_at::date` could
--              reconstruct months later once older detail rows have
--              themselves been rolled up, pruned, or moved. `computed_at`
--              records when a given (day, tenant) row was last (re)computed,
--              since a job may recompute "today" repeatedly as the day
--              progresses (see PRIMARY KEY note below).
--
--              PRIMARY KEY (day, tenant): a later PR's rollup job is expected
--              to upsert (`ON CONFLICT (day, tenant) DO UPDATE`) rather than
--              insert-only, since "today"'s row is recomputed repeatedly
--              (intraday) until the day is over, at which point it becomes
--              historical and stable.
--
--              ── platform_active_users ──
--              Per-user activity PRESENCE, one row per (day, user, tenant,
--              source) — the grain exists specifically so a user active in
--              two orgs on the same day is counted once per org, not
--              collapsed into a single ambiguous "active" bit, while a
--              platform-wide "distinct active users today" query can still
--              `SELECT COUNT(DISTINCT user_id)` across every tenant without
--              double-counting that same user. `source` records WHICH
--              signal produced the presence row (`access` = a login,
--              `activity` = an in-product action, `query` = an ask/search
--              call, `mcp` = an MCP tool call) so different activity
--              definitions can be composed later without re-deriving from
--              raw per-tenant tables.
--
--              No FK on `user_id` — deliberately, and for a DIFFERENT reason
--              than `member_ocean_lifecycle_log.subject_user_id`'s
--              survive-deletion rationale (0092): this table is populated by
--              aggregating rows OUT OF EACH TENANT DATABASE, where `users`
--              does not exist at all (`users` is control-plane-only; see
--              `account_keys` in
--              migrations_tenant/0002_tenant_meta_and_account_keys.sql for
--              the identical "no FK possible, the referenced table lives in
--              a different database" situation, there for `account_id`
--              rather than `user_id`). A `REFERENCES users(id)` here would
--              be structurally sound for the `'__consumer__'` tenant's rows
--              (where `users` really is a sibling table in this same
--              database) but wrong for every org-tenant row this table also
--              has to hold, so the column is a bare `uuid` for both.
--
--              PRIMARY KEY (day, user_id, tenant, source): the natural key
--              for "was this user active, via this signal, in this tenant,
--              on this day" — a later PR's fan-in job upserts
--              (`ON CONFLICT ... DO NOTHING`, presence has no payload to
--              update) per source per tenant per day.
--
--              ── Not yet written by any code path in this PR ──
--              This PR is schema + purge worker only. `user_access_log`
--              gets its first writer in a later PR (the login/session
--              middleware); `platform_metrics_daily` and
--              `platform_active_users` get their first writers in the
--              rollup-job PR(s) later in this series. All three tables are
--              additive and inert until those PRs land.
--
--              ── CI note ──
--              All three table names MUST be added to `CONTROL_TABLES` in
--              `scripts/check_schema_parity.sh` in this SAME PR (done) —
--              that script diffs a schema-only `pg_dump` of the control tree
--              (minus `CONTROL_TABLES`) against the tenant baseline, and
--              these three tables have no tenant-tree counterpart at all.
--
-- Depends on: 0001_init (users), 0092_member_ocean_control_tables
--             (the "control-only table, no tenant-tree twin, ever" precedent
--             this migration follows), 0002_tenant_meta_and_account_keys.sql
--             on the tenant tree (the no-cross-database-FK precedent for
--             `platform_active_users.user_id`).
--
-- Down:
--   DROP INDEX IF EXISTS user_access_log_user_created_idx;
--   DROP INDEX IF EXISTS user_access_log_created_idx;
--   DROP TABLE IF EXISTS user_access_log;
--   DROP TABLE IF EXISTS platform_metrics_daily;
--   DROP TABLE IF EXISTS platform_active_users;

CREATE TABLE user_access_log (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    clerk_session_id TEXT        NOT NULL,
    client           TEXT,                  -- nullable: older app builds send nothing
    ip               INET,
    country          TEXT,
    region           TEXT,
    city             TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One row per login: the middleware's insert is an
    -- `ON CONFLICT (user_id, clerk_session_id) DO NOTHING`.
    CONSTRAINT user_access_log_user_session_uq UNIQUE (user_id, clerk_session_id)
);

-- Purge worker's own access pattern: delete everything older than the cutoff.
CREATE INDEX user_access_log_created_idx ON user_access_log (created_at);
-- Operator/analytics access pattern: one user's login history, newest first.
CREATE INDEX user_access_log_user_created_idx ON user_access_log (user_id, created_at DESC);

CREATE TABLE platform_metrics_daily (
    day               DATE        NOT NULL,
    tenant            TEXT        NOT NULL,  -- '__consumer__', else clerk_org_id
    atom_count        BIGINT,
    reef_count        BIGINT,
    session_count     BIGINT,
    llm_input_tokens  BIGINT,
    llm_output_tokens BIGINT,
    computed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (day, tenant)
);

CREATE TABLE platform_active_users (
    day     DATE NOT NULL,
    user_id UUID NOT NULL,     -- no FK: aggregated from tenant DBs where `users` doesn't exist
    tenant  TEXT NOT NULL,     -- '__consumer__', else clerk_org_id
    source  TEXT NOT NULL CHECK (source IN ('access', 'activity', 'query', 'mcp')),
    PRIMARY KEY (day, user_id, tenant, source)
);
