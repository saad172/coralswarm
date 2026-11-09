-- Migration 0180: google_watch_channels (gsuite Train G1 PR 2,
--   `.scratch/gsuite-enterprise/plan.md` decision 7 — push, not polling).
--
--   Persistence for Google Workspace push-notification registrations that
--   `workers::google_watch_renewal` renews before they lapse: Gmail
--   `users.watch` (Pub/Sub push, ~7-day expiry), a Workspace Events API
--   subscription (also Pub/Sub push, Chat), and a Calendar `events.watch`
--   webhook channel (direct push, no Pub/Sub). No per-surface ingest code
--   exists yet — this PR is infrastructure spine only (G1 PR 2). G2 (Gmail),
--   G3 (Calendar + Meet), and G4 (Chat) populate rows here as each adds its
--   own watch-creation call; this table and the renewal worker are written
--   ahead of that so a lapsed watch is never a day-one risk once they do
--   (a lapsed watch is silent data loss — engineering obligation in the
--   plan, "Pub/Sub push endpoint must verify... watch renewals must alert
--   on failure").
--
--   ── Structural precedent ──
--   Mirrors `graph_subscriptions` (0035_outlook_thread_watch.sql) almost
--   exactly: one row per (account, watched resource), account-scoped,
--   tracked for renewal by expiry, no ocean/graph anchor. That precedent is
--   also why this table's RLS story on the TENANT tree is "deliberately not
--   covered" — see the tenant-twin migration's header
--   (`migrations_tenant/0062_google_watch_channels.sql`) and
--   `migrations_tenant/0009_tenant_rls.sql`'s own "(f) NOT COVERED" section,
--   which lists `graph_subscriptions` for the identical reason: no
--   `graph_id` to anchor a member-ocean-isolation policy against.
--
--   ── Column choices ──
--   `surface` — 'gmail' | 'chat' | 'calendar' (CHECK, not an enum — same
--   "free TEXT key" call `org_connector_flags.provider` made, for the same
--   reason: this is routing/bookkeeping, not ocean content).
--
--   `resource_id` — the surface-specific thing being watched: Gmail's
--   watched mailbox (conventionally "me"), the Workspace Events API target
--   resource name for a Chat subscription, or the Calendar id (usually
--   "primary"). Free TEXT — Google-owned shape, no local FK.
--
--   `channel_id` / `channel_resource_id` — Calendar-only. A Calendar
--   `events.watch` channel is identified by a caller-chosen `id` (our
--   `channel_id`) plus Google's own `resourceId` for the watched resource
--   (our `channel_resource_id`, needed to `channels.stop` a superseded
--   channel before creating its replacement — Calendar channels cannot be
--   renewed in place, only recreated). Empty string (not NULL) for
--   gmail/chat, matching this repo's "unused id columns default to ''"
--   convention (see `teams_watched_resources`' header) so the unique key
--   below dedupes correctly.
--
--   `topic_name` — the Pub/Sub topic a Gmail/Chat watch publishes
--   notifications to (`projects/<project>/topics/gmail-push` or
--   `chat-events`, per the provisioning script,
--   `scripts/provision_google_workspace.sh`). Empty for calendar (no
--   Pub/Sub involved — Calendar pushes directly to our webhook).
--
--   `channel_token` — the opaque secret Google echoes back verbatim on every
--   Calendar channel notification (`X-Goog-Channel-Token` header),
--   constant-time-verified the same way `graph_subscriptions.client_state`
--   is verified against Graph notifications. Unused for gmail/chat, which
--   are authenticated via the Pub/Sub push envelope's OIDC bearer token
--   instead (`routes::google_push`) — Google Pub/Sub push has no per-message
--   shared-secret field of its own.
--
--   `last_error` / `last_error_at` — set by the renewal worker on a failed
--   renew attempt (never cleared implicitly; the next successful renewal
--   clears both). This is the loud-failure trail the plan's "must alert on
--   failure" obligation reads from — see `workers::google_watch_renewal`'s
--   module doc for where it logs at `error!` level and how a future paging
--   hook would consume it.
--
--   ── Primary key + uniqueness ──
--   Surrogate `id UUID PRIMARY KEY`, plus `UNIQUE (account_id, surface,
--   resource_id)` — one active watch per account per surface per watched
--   resource, the same shape `graph_subscriptions`' `UNIQUE (account_id,
--   resource)` uses.
--
-- Depends on: none (bare table, only an `accounts` FK).
-- Down:
--   DROP INDEX IF EXISTS idx_google_watch_channels_expiry;
--   DROP TABLE IF EXISTS google_watch_channels;

CREATE TABLE IF NOT EXISTS google_watch_channels (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
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

-- Renewal worker scans for channels nearing expiry (mirrors
-- idx_graph_subscriptions_expiry).
CREATE INDEX IF NOT EXISTS idx_google_watch_channels_expiry
    ON google_watch_channels (expires_at);

-- Account-scoped bookkeeping on the shared multi-account consumer/control
-- DB: plain account_isolation, same predicate + shape as graph_subscriptions'
-- own policy in 0083_consumer_rls.sql (not added there since this table
-- postdates it — same "individual migration adds its own account_isolation
-- policy" pattern several post-0083 control migrations already use, e.g.
-- 0101_slack_watched_channels.sql, 0106_team_seat_grants.sql).
ALTER TABLE google_watch_channels ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON google_watch_channels;
CREATE POLICY account_isolation ON google_watch_channels
    USING (account_id = current_setting('app.account_id')::uuid);
