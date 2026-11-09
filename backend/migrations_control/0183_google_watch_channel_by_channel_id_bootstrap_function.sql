-- Migration 0183: google_watch_channels lookup-by-channel_id bootstrap
-- function (gsuite Train G3 PR 1, discovered while wiring the Calendar push
-- webhook — same "found mid-PR, taken without pre-announcement, recorded
-- after the fact" class as 0181, `resolve_google_watch_channels_due`).
--
--   `routes::google_calendar_push` (the Calendar `events.watch` webhook —
--   NOT the Pub/Sub `routes::google_push`) authenticates an inbound
--   notification with ONLY what Google's `X-Goog-Channel-*` headers carry: a
--   bare `channel_id` and a token to verify against the stored secret. There
--   is no `account_id` yet — it has to be DISCOVERED from the channel id
--   before any `begin_tenant_system_scoped` transaction can be opened,
--   exactly the same bootstrap problem `0086_webhook_share_bootstrap_
--   functions.sql`'s `resolve_graph_subscription_account_id` solves for
--   Graph's `clientState`-authenticated webhook, and `0181`'s
--   `resolve_google_watch_channels_due` solves for the cross-account
--   renewal sweep. This is that same pattern, keyed by `channel_id` instead
--   of `subscription_id`/expiry: bare `account_id` only — never
--   `channel_token` (the notification-validation secret itself) — so the
--   secret comparison still happens against a value read back inside a
--   proper scoped transaction for the discovered account, not against
--   anything this function returns.
--
--   Control/consumer-plane only, no tenant-tree twin — same reasoning as
--   0181's header: an org tenant database holds exactly one account
--   (ADR-0007), so there is no cross-account discovery problem there.
--   Registered in `scripts/check_schema_parity.sh`'s `CONTROL_FUNCTIONS`
--   list, same as 0181.
--
--   `p_channel_id <> ''` (CodeRabbit review finding on PR #677, Major/
--   security, fixed before founder review): a `google_watch_channels` row
--   created by a sync-only write, before `ensure_watch` has ever succeeded
--   (`connectors::google_calendar::upsert_sync_columns`'s own doc), defaults
--   `channel_id`/`channel_token` to `''` (the column defaults). Without this
--   guard, an inbound request carrying an EMPTY `X-Goog-Channel-ID` would
--   resolve to WHICHEVER account currently has such a placeholder row —
--   trivially forgeable, no secret required. The Rust caller
--   (`connectors::google_calendar::resolve_channel_for_push`) also rejects
--   an empty `channel_token` on the row it reads back, and
--   `routes::google_calendar_push` rejects empty headers before ever
--   calling either — three independent layers, deliberately redundant.
--
-- Depends on: 0180_google_watch_channels.sql (the table this reads).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_google_watch_channel_account_id(text) FROM coralswarm_app, CURRENT_USER;
--   REVOKE EXECUTE ON FUNCTION resolve_google_watch_channel_account_id(text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_google_watch_channel_account_id(text);

CREATE OR REPLACE FUNCTION resolve_google_watch_channel_account_id(p_channel_id text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT gwc.account_id
    FROM google_watch_channels gwc
    WHERE gwc.surface = 'calendar'
      AND gwc.channel_id = p_channel_id
      AND p_channel_id <> ''
      AND gwc.channel_id <> ''
    LIMIT 1
$$;

REVOKE EXECUTE ON FUNCTION resolve_google_watch_channel_account_id(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_google_watch_channel_account_id(text) TO coralswarm_app, CURRENT_USER;
