-- Migration: 0227_slack_unmatched_email_notices
-- Description: Chat-per-user plan PR 6 ("email the unmatched",
--   docs/plans/chat-per-user-ingest-and-in-chat-ask.md §8 row 6 / §11
--   correction 4) — the persisted, per-recipient cooldown that replaces the
--   in-process courtesy-notice cooldown for this one new send path.
--
--   ── Why a NEW table, not reusing `should_send_unmatched_notice` ──
--   `connectors::slack::should_send_unmatched_notice`
--   (`connectors/slack.rs`, `UNMATCHED_NOTICE_COOLDOWN`) is an in-process
--   `HashMap<(Uuid, String), Instant>`: per backend machine, reset on every
--   deploy, and keyed by (account, CONTAINER) rather than by recipient. Its
--   own doc comment already names the trade and the fix: "a deliberate trade
--   against a schema change ... a persisted `last_notified_at` is the
--   correct long-term home if the connector ever runs wide." For an
--   in-channel courtesy line landing in a Slack channel that trade is fine —
--   worst case, a channel hears the same courtesy line again after a deploy.
--   For a real inbox it is not: the SAME container's roster is re-walked on
--   every message while it has zero destinations, on every backend restart,
--   and on every re-deploy, so an unpersisted per-container cooldown would
--   re-email the same real person every time the process restarts.
--
--   This table fixes BOTH gaps at once: persisted (survives a restart/
--   deploy) and keyed by RECIPIENT (`account_id, provider, email`), not by
--   container — a person who is `Unmatched` in five channels the agent
--   shares with them gets at most one nudge per cooldown window, not five.
--   `workspace_id`/`channel_id` are carried for observability only (which
--   container's roster walk most recently triggered a claim), never part of
--   the uniqueness/cooldown key.
--
--   ── The cooldown window: 7 days, not 6 hours ──
--   `UNMATCHED_NOTICE_COOLDOWN` (6h) is calibrated for a courtesy line meant
--   to be seen "once a shift" in a channel nobody is required to read. An
--   email is a real inbox interruption; repeating it every 6 hours would be
--   spam, not a courtesy. 7 days gives someone a full work week to notice
--   and sign up before a second nudge, while still eventually reminding
--   someone who missed the first one — see `services::unmatched_recipient_
--   email::COOLDOWN` for the single source of truth this migration's own
--   comment mirrors.
--
--   ── `provider`, not Slack-hardcoded ──
--   The plan's spine (§1) is "one structure, built once" across every chat
--   surface. This table has no Slack-specific column (email + a cooldown
--   timestamp is the whole shape), so it costs nothing to key it
--   `(account_id, provider, email)` now rather than assume Slack forever and
--   need a migration to widen it the day a second platform's discovery
--   module (Teams/Google Chat) wants the identical cooldown. Only Slack
--   writes it today — `DEFAULT 'slack'` documents that without forcing every
--   INSERT to spell it out.
--
--   ── RLS: mirrors `connector_audit_log` (0193), not `slack_watched_
--   channels` ──
--   This table has no `graph_id`/ocean concept at all — it is account-level
--   operational state (who has already been nudged, and when), exactly like
--   `connector_audit_log`'s `subject`/`detail` columns. So it gets that
--   table's RLS shape (a bare account-scoped `account_isolation` policy)
--   rather than `slack_watched_channels`'/`google_chat_watched_spaces`'
--   `graph_id`-anchored `member_ocean_isolation`. Unlike `connector_audit_log`
--   this table is NOT append-only (the whole point is an atomic UPSERT that
--   claims-as-it-reports — see the worker's own doc comment for the exact
--   `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE ... RETURNING` shape), so
--   it gets ONE bare `account_isolation` policy covering every command,
--   rather than `connector_audit_log`'s SELECT/INSERT command split.
--
--   ── Control-only, no tenant-tree twin (corrected 2026-09-20; see 0228's
--   header for the incident this fixes) ──
--   An earlier version of this migration shipped a tenant-tree twin
--   (`migrations_tenant/0093`, RLS-restricted to `tenant_rls_is_system()`),
--   on the theory that `begin_tenant_system_scoped(pool, account_id)` "sets
--   both planes' GUCs in one call, so the worker never has to know which
--   plane it's on" — true as far as it went, but it also meant an ORG
--   account's row landed in that org's own tenant database, which
--   `routes::email_unsubscribe`'s public, unauthenticated caller (no
--   `Tenant`/`TenancyDecision` to pick a plane with) could never resolve a
--   token against. That twin is deleted (never merged or applied anywhere)
--   in favour of this: a send-suppression list is bookkeeping about an EMAIL
--   ADDRESS and an ACCOUNT — no `graph_id`, no ocean anchor, never read back
--   as content — which puts it in the same control-only category as
--   `oauth_tokens`/`slack_integrations` (both control-only for the identical
--   reason), same posture as `microsoft_admin_consents`'s own "Control-only,
--   no tenant-tree twin" section (migration 0188). `workers::
--   slack_unmatched_email::process_job` now opens its
--   `begin_tenant_system_scoped(control_pool, account_id)` transaction on the
--   shared control pool UNCONDITIONALLY, regardless of which plane that
--   tick's own `t.pool` is for — see that worker's own module doc. Added to
--   `CONTROL_TABLES` (not `CONTROL_ONLY_RLS_TABLES`) in `scripts/
--   check_schema_parity.sh` in this same PR.
--
-- Depends on: 0083_consumer_rls (the `account_isolation`-shaped policy
--             convention), 0193_connector_audit_log (the direct-account_id,
--             no-graph-anchor RLS precedent this migration follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON slack_unmatched_email_notices;
--   DROP TABLE IF EXISTS slack_unmatched_email_notices;

CREATE TABLE IF NOT EXISTS slack_unmatched_email_notices (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    provider          TEXT        NOT NULL DEFAULT 'slack',
    -- Normalized (lowercase + trim only — connectors::slack::normalize_email's
    -- rule, NOT provider canonicalisation) verified email of the recipient.
    email             TEXT        NOT NULL CHECK (email = lower(btrim(email)) AND email <> ''),
    -- Observability only — the container whose roster walk most recently
    -- claimed/renewed this row. Never part of the cooldown key (see header).
    workspace_id      TEXT        NOT NULL,
    channel_id        TEXT        NOT NULL,
    last_notified_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, provider, email)
);

ALTER TABLE slack_unmatched_email_notices ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON slack_unmatched_email_notices;
CREATE POLICY account_isolation ON slack_unmatched_email_notices
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
