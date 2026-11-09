-- Migration: 0030_extraction_cache
-- Description: `extraction_cache` — the TENANT half of control's
--   `0122_extraction_cache`. See that migration's header for the full design
--   rationale (why `prompt_version` is part of the key, why `account_id` is
--   part of the `UNIQUE` constraint, why `response_content` is `TEXT`, why
--   `hit_count`/`last_hit_at` exist) — not repeated here to avoid the two
--   copies drifting out of sync in prose while the DDL itself is kept
--   byte-equivalent (minus the FK, per this file's own section below).
--
-- PAIRED WITH: migrations_control/0122_extraction_cache.sql — same table
--   shape, minus the `accounts` FK that does not exist in a tenant DB (the
--   `0106_team_seat_grants` / `0017_team_seat_grants` convention — see that
--   pair's headers for the general rule).
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THIS TABLE CARRIES NO ROW-LEVEL SECURITY AT ALL ON THIS PLANE
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Every other shared content table added post-`0009_tenant_rls` gets a
-- `member_ocean_isolation` policy anchored on `graph_id` through
-- `tenant_rls_visible_graph_ids()` — the boundary between one member's
-- private ocean and another's INSIDE the one org this database holds.
-- `extraction_cache` has no `graph_id` column and cannot be given that
-- policy, because the whole point of this table on this plane is to be
-- reachable from EVERY ocean in the tenant: the same Slack message fanned
-- into N member-private oceans must hit the SAME cached row regardless of
-- which member's ocean is asking. A `graph_id`-anchored (or any per-member)
-- policy here would silently defeat the fan-out economics this table exists
-- to capture — every member's own extraction would look like a fresh, unique
-- atom to a policy that scoped by ocean.
--
-- This is the same class of "no ocean anchor at all" table
-- `check_schema_parity.sh`'s own comment calls out for `queries`/
-- `graph_subscriptions`/`oauth_tokens`/`slack_*` (control-covered by 0083,
-- deliberately left OFF `migrations_tenant/0009_tenant_rls`'s roster) — see
-- that script's `TENANT_ONLY_RLS_TABLES` comment. `extraction_cache` follows
-- the identical precedent, just from the opposite direction: covered on
-- CONTROL (real per-account boundary to enforce), absent from TENANT (no
-- boundary to enforce; whole-database isolation between orgs is the only
-- boundary that applies, and it is already physical).
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `account_id` STILL EXISTS HERE, ALWAYS HOLDING THE SAME SENTINEL VALUE
-- ─────────────────────────────────────────────────────────────────────────────
--
-- `scripts/check_schema_parity.sh` requires the shared table's SHAPE
-- (columns, types, nullability, constraints) to match byte-for-byte across
-- both trees — only the RLS/policy DDL layered on top is allowed to differ.
-- Dropping `account_id` here (there being no real "account" concept on the
-- org-tenant plane) would break that parity check for a column the control
-- side genuinely needs. Keeping the column and its role in the `UNIQUE`
-- constraint, and always writing the SAME fixed sentinel UUID
-- (`00000000-0000-0000-0000-000000000000`, i.e. `Uuid::nil()` — see
-- `crate::workers::embed::JobScope::Org` callers of
-- `crate::llm::extraction_cache`) collapses `UNIQUE (account_id,
-- content_hash, task, model, prompt_version)` to exactly the bare
-- `(content_hash, task, model, prompt_version)` key the design calls for on
-- this plane: every org-tenant row shares the one sentinel, so two rows can
-- only collide (and therefore dedupe) when their real key columns match —
-- true DB-wide sharing, achieved with the identical constraint shape as the
-- control side, no NULL-coalescing index expression or divergent constraint
-- needed.
--
-- Depends on: 0001_tenant_baseline (graphs — no FK from this table, but the
--   worker call sites that write it hold a `graph_id` in scope), 0006's
--   sibling precedent for the FK-omission convention
--   (`0017_team_seat_grants`).
--
-- Down:
--   DROP TABLE IF EXISTS extraction_cache;

CREATE TABLE IF NOT EXISTS extraction_cache (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- No FK: accounts is control-only, does not exist in a tenant DB. Always
    -- the fixed sentinel `00000000-0000-0000-0000-000000000000` on this
    -- plane — see this migration's header.
    account_id                  UUID        NOT NULL,
    content_hash                TEXT        NOT NULL CHECK (content_hash <> ''),
    task                        TEXT        NOT NULL CHECK (task <> ''),
    model                       TEXT        NOT NULL CHECK (model <> ''),
    prompt_version              SMALLINT    NOT NULL,
    response_content            TEXT        NOT NULL CHECK (response_content <> ''),
    input_tokens                INTEGER     NOT NULL DEFAULT 0,
    output_tokens               INTEGER     NOT NULL DEFAULT 0,
    cache_creation_input_tokens BIGINT      NOT NULL DEFAULT 0,
    cache_read_input_tokens     BIGINT      NOT NULL DEFAULT 0,
    hit_count                   INTEGER     NOT NULL DEFAULT 0,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_hit_at                 TIMESTAMPTZ,

    UNIQUE (account_id, content_hash, task, model, prompt_version)
);

-- Deliberately NO `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` and no policy —
-- see this migration's header.
