-- Migration 0077: google_meet_sync_state (tenant twin of
--   migrations_control/0206_google_meet_sync_state.sql — gsuite Train G3
--   PR 2, schema-parity twin pattern per m365 Train 2 PR 2.2/#661's
--   precedent, same as `google_watch_channels`' own control/tenant pair).
--
--   Byte-identical column set to the control-tree table; see that
--   migration's header for the full column-by-column rationale. `account_id`/
--   `owner_user_id` are bare `UUID` here, no FKs — `accounts`/`users` are
--   control-plane-only tables (`scripts/check_schema_parity.sh`'s
--   `CONTROL_TABLES` list), same tenant-tree convention every other twin in
--   this train follows.
--
--   ── RLS: deliberately NOT covered (same call as google_watch_channels) ──
--   No `graph_id`/ocean anchor, so there is nothing to write a
--   `member_ocean_isolation` policy against — the exact situation
--   `migrations_tenant/0009_tenant_rls.sql`'s "(f) NOT COVERED" section
--   documents for `graph_subscriptions`, and `migrations_tenant/
--   0062_google_watch_channels.sql`'s own header restates for that table. An
--   org tenant database holds exactly one account (ADR-0007), so there is
--   also no `account_isolation`-style policy to add here either — that
--   predicate exists to separate MANY accounts sharing one physical consumer
--   DB, which is not this database's situation.
--
--   In practice this table is never written on a genuine org tenant pool at
--   all in this PR: `google_meet` OAuth can never complete under an org
--   account (`routes::oauth::google_org_callback_allowed` refuses every
--   non-Gmail Google surface unconditionally — see `google-workspace.md` §2),
--   so no `google_meet_sync_state` row is ever created there. The schema
--   twin exists for the same reason every other control+tenant pair in this
--   train carries one: under Personal tenancy, control and content are the
--   SAME physical database (`tenancy::extractor::Tenant`'s own doc), so this
--   migration set must apply cleanly there too, and schema-parity tooling
--   (`scripts/check_schema_parity.sh`) expects a twin for every control-tree
--   table that is not explicitly listed as control-only.
--
-- Depends on: none (bare table; account_id/owner_user_id are soft cross-DB
--             references).
-- Down:
--   DROP TABLE IF EXISTS google_meet_sync_state;

CREATE TABLE IF NOT EXISTS google_meet_sync_state (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id             UUID        NOT NULL,
    owner_user_id          UUID        NOT NULL,
    last_synced_end_time   TIMESTAMPTZ,
    claimed_until          TIMESTAMPTZ,
    claim_id               UUID,
    last_attempted_at      TIMESTAMPTZ,
    last_synced_at         TIMESTAMPTZ,
    last_error             TEXT,
    last_error_at          TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, owner_user_id)
);
