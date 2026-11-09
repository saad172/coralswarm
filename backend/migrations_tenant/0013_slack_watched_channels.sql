-- Migration: 0013_slack_watched_channels (tenant tree)
-- Description: Tenant-tree half of the paired
--   `migrations_control/0101_slack_watched_channels.sql` — see that file for
--   the full rationale (the per-channel Slack destination table, why the
--   org-account Slack ingest path has been fail-closed dead without a
--   per-channel read path, and why this table currently has no write side —
--   it is populated only by the future destination picker, not by any code
--   shipped in this migration's PR).
--
--   `slack_watched_channels` is a NEW shared content table (not a column
--   add), so this file's `CREATE TABLE` must stay byte-equivalent to the
--   control side's shape or `scripts/check_schema_parity.sh` fails: it
--   applies each tree to a scratch DB and diffs a normalized
--   `pg_dump --schema-only` of the shared tables (RLS/policy DDL stripped
--   from both sides first — see that script's `CONTROL_ONLY_RLS_TABLES`/
--   `TENANT_ONLY_RLS_TABLES`, which this table is added to in the same PR).
--
--   `account_id`/`user_id` carry NO foreign key here, unlike the control
--   side — `accounts`/`users` are control-only tables (`CONTROL_TABLES` in
--   `check_schema_parity.sh`) that do not exist in a tenant database at all,
--   so a `REFERENCES accounts(id)` here would fail to apply outright. This
--   is not a divergence the parity script would catch as a mismatch: its
--   control-side reduction step DROPs `accounts`/`users` `CASCADE`, which
--   also drops any FK constraint depending on them — so the control side's
--   `account_id`/`user_id` FKs vanish from the comparison the same way this
--   tree's bare columns never had them. `teams_watched_resources`
--   (`0001_tenant_baseline.sql`) is the exact precedent: bare
--   `account_id uuid NOT NULL` / `user_id uuid NOT NULL`, while `graph_id`
--   keeps its FK to `graphs` (a table that DOES exist here) both sides.
--
--   ── Why this table gets REAL tenant RLS, unlike `slack_integrations`/
--   `slack_huddle_captures` ──
--   Those two sit in this baseline already but carry no
--   `member_ocean_isolation` policy at all (see this file's own
--   `DELIBERATELY_UNCOVERED` roster in `backend/tests/tenant_rls_pg.rs`) —
--   they are genuinely inert on the org/tenant plane today. Copying that
--   would be wrong here: `slack_watched_channels.graph_id` is precisely the
--   per-member/per-team ocean boundary this migration's other policies
--   enforce, so it gets the exact same treatment as
--   `teams_watched_resources` below — anchored on `graph_id` through
--   `tenant_rls_visible_graph_ids()`, not on `account_id` (every row in one
--   tenant DB shares the same single account/org already; the boundary that
--   matters here is WHICH member's/team's ocean a channel feeds).
--
-- Depends on: 0001_tenant_baseline (accounts/users/graphs shapes),
--             0009_tenant_rls (coralswarm_tenant_app role,
--             `tenant_rls_visible_graph_ids()`, the `teams_watched_resources`
--             precedent this mirrors).
--
-- Paired with: migrations_control/0101_slack_watched_channels.sql.
--
--   ── AMENDED BEFORE SHIPPING: `mode` discriminator + nullable `graph_id` ──
--   Mirrors an in-place edit to the paired control migration — see that
--   file's own "AMENDED BEFORE SHIPPING" header section for the full
--   rationale (the org picker's "everyone's individual oceans" pick has no
--   single destination ocean to store, so `graph_id UUID NOT NULL` had no
--   way to represent it). Neither migration has ever shipped, so both are
--   edited in place rather than patched by a follow-on migration.
--
--   The `CREATE TABLE` below stays byte-equivalent to the control side's
--   (per this file's own header above): `mode`/the nullable `graph_id`/the
--   new CHECK constraint are added identically on both sides. The
--   `member_ocean_isolation` policy below is UNCHANGED — writes to this
--   table only ever happen on the control/consumer pool today (no per-org
--   tenant-DB routing exists in the Slack ingest funnel), so there is no
--   live path that could attempt to write a NULL-`graph_id` 'fanout' row
--   through this policy. If a tenant-plane writer is ever added, widen this
--   policy the same NULL-friendly way the control side's `WITH CHECK` was
--   widened, at that time — not before, since an untested, unreachable
--   widening is not a safety improvement.
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON slack_watched_channels;
--   ALTER TABLE slack_watched_channels DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS slack_watched_channels;

CREATE TABLE IF NOT EXISTS slack_watched_channels (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- No FK: accounts is control-only, does not exist in a tenant DB.
    account_id       UUID        NOT NULL,
    -- No FK: users is control-only, does not exist in a tenant DB.
    user_id          UUID        NOT NULL,
    -- Mode discriminator — see this file's "AMENDED BEFORE SHIPPING" header
    -- section. Must stay byte-equivalent to the control side's shape.
    mode             TEXT        NOT NULL DEFAULT 'single'
                                 CHECK (mode IN ('single', 'fanout')),
    -- NULL exactly when mode = 'fanout' — enforced by the CHECK below.
    graph_id         UUID        REFERENCES graphs(id) ON DELETE CASCADE,
    workspace_id     TEXT        NOT NULL,
    channel_id       TEXT        NOT NULL,
    display_name     TEXT        NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Renamed from `last_ingested_at` — see the paired control migration's
    -- header for why (bumped at resolution time, not ingest time; no reader
    -- exists yet). Must stay byte-equivalent to the control side's shape.
    last_resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, workspace_id, channel_id),

    -- Ties `mode` to `graph_id`'s nullability — must stay byte-equivalent
    -- to the control side's shape.
    CONSTRAINT slack_watched_channels_mode_graph_id_check CHECK (
        (mode = 'single' AND graph_id IS NOT NULL)
        OR (mode = 'fanout' AND graph_id IS NULL)
    )
);

ALTER TABLE slack_watched_channels ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON slack_watched_channels;
CREATE POLICY member_ocean_isolation ON slack_watched_channels
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
