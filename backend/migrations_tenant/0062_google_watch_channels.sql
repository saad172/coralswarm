-- Migration 0062: google_watch_channels (tenant twin of
--   migrations_control/0180_google_watch_channels.sql — gsuite Train G1 PR 2,
--   schema-parity twin pattern per m365 Train 2 PR 2.2/#661's precedent).
--
--   Byte-identical column set to the control-tree table; see that
--   migration's header for the full column-by-column rationale. The only
--   structural difference (required by the tenant-tree convention — see
--   `docs/plans/org-tenancy-implementation-plan.md`'s "In the tenant
--   baseline, FKs to users/accounts become bare uuid columns") is
--   `account_id`: a bare `UUID`, no `REFERENCES accounts(id)`, since
--   `accounts` is a control-plane-only table (`scripts/
--   check_schema_parity.sh`'s `CONTROL_TABLES` list) — `check_schema_parity.
--   sh` strips control-only tables via `DROP TABLE ... CASCADE` before
--   diffing, so the control tree's FK is invisible to the parity diff
--   either way, but the tenant tree never carries FKs to tables it does not
--   have.
--
--   ── RLS: deliberately NOT covered (same call as graph_subscriptions) ──
--   This table has no `graph_id`/ocean anchor, so there is nothing to write
--   a `member_ocean_isolation` policy against — the exact situation
--   `migrations_tenant/0009_tenant_rls.sql`'s "(f) NOT COVERED" section
--   already documents for `graph_subscriptions` ("despite the name, no
--   graph_id... nothing to anchor"). An org tenant database holds exactly
--   one account (ADR-0007), so there is also no `account_isolation`-style
--   policy to add here the way the control tree's twin migration does —
--   that predicate exists to separate MANY accounts sharing one physical
--   consumer DB, which is not this database's situation at all. So, same as
--   `graph_subscriptions` on the tenant tree: no `ALTER TABLE ... ENABLE ROW
--   LEVEL SECURITY`, no policy, by design.
--
-- Depends on: none (bare table; account_id is a soft cross-DB reference).
-- Down:
--   DROP INDEX IF EXISTS idx_google_watch_channels_expiry;
--   DROP TABLE IF EXISTS google_watch_channels;

CREATE TABLE IF NOT EXISTS google_watch_channels (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID        NOT NULL,
    surface              TEXT        NOT NULL CHECK (surface IN ('gmail', 'chat', 'calendar')),
    resource_id          TEXT        NOT NULL,
    channel_id           TEXT        NOT NULL DEFAULT '',
    channel_resource_id  TEXT        NOT NULL DEFAULT '',
    topic_name           TEXT        NOT NULL DEFAULT '',
    channel_token        TEXT        NOT NULL DEFAULT '',
    expires_at           TIMESTAMPTZ NOT NULL,
    last_renewed_at       TIMESTAMPTZ,
    last_error            TEXT,
    last_error_at         TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, surface, resource_id)
);

CREATE INDEX IF NOT EXISTS idx_google_watch_channels_expiry
    ON google_watch_channels (expires_at);
