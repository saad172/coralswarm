-- Migration: 0253_teams_watched_per_member
-- Description: CS-456 / Phase G wave 2 item 47a (S4) — one
--   `teams_watched_resources` row per member for the same channel/chat on the
--   delegated path. The table previously unique'd on
--   `(account_id, kind, team_id, channel_id, chat_id)`, which made a second
--   org member opting into the same container either 409 or (pre-PR-#668)
--   silently reassign the row. Under D21 every Teams READ uses that member's
--   own token, so each watcher needs their own row.
--
--   App-mode (RSC) installs (item 48 / wave 5 retires them) stay ONE row per
--   container: a partial unique index on the container columns WHERE
--   `auth_mode = 'app'` keeps the install path's insert from colliding once
--   `user_id` is in the delegated key. Delegated rows use a separate partial
--   unique that INCLUDES `user_id`. `user_id` is NOT NULL on both modes
--   (app-mode uses a bookkeeping representative — see `install_watch`), so
--   no sentinel / shared-null trick is needed.
--
--   Twin: `migrations_tenant/0104_teams_watched_per_member.sql` (same table
--   exists on both planes — see 0037 + tenant baseline / 0009 RLS).
--
-- Depends on: 0037_teams_watched_resources, 0038_teams_rsc_auth (auth_mode).
-- Down:
--   DROP INDEX IF EXISTS teams_watched_resources_delegated_member_uq;
--   DROP INDEX IF EXISTS teams_watched_resources_app_container_uq;
--   ALTER TABLE teams_watched_resources
--     ADD CONSTRAINT teams_watched_resources_account_id_kind_team_id_channel_id_chat_id_key
--     UNIQUE (account_id, kind, team_id, channel_id, chat_id);

ALTER TABLE teams_watched_resources
    DROP CONSTRAINT IF EXISTS teams_watched_resources_account_id_kind_team_id_channel_id_chat_id_key;
-- Postgres auto-truncates long constraint names; the original CREATE TABLE
-- UNIQUE produced this truncated name on both planes:
ALTER TABLE teams_watched_resources
    DROP CONSTRAINT IF EXISTS teams_watched_resources_account_id_kind_team_id_channel_id__key;

-- Per-member delegated watches (S4).
CREATE UNIQUE INDEX IF NOT EXISTS teams_watched_resources_delegated_member_uq
    ON teams_watched_resources (account_id, kind, team_id, channel_id, chat_id, user_id)
    WHERE auth_mode = 'delegated';

-- App-mode RSC installs: still one row per container until item 48 retires them.
CREATE UNIQUE INDEX IF NOT EXISTS teams_watched_resources_app_container_uq
    ON teams_watched_resources (account_id, kind, team_id, channel_id, chat_id)
    WHERE auth_mode = 'app';
