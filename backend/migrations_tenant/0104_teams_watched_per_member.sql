-- Migration: 0104_teams_watched_per_member
-- Description: Tenant-plane twin of control `0253_teams_watched_per_member`
--   (CS-456 / Phase G wave 2 item 47a / S4). Same rationale — see that file.
--   Constraint name on the tenant baseline dump is the truncated form
--   `teams_watched_resources_account_id_kind_team_id_channel_id__key`.
--
-- Depends on: 0001_tenant_baseline (table + unique), auth_mode from baseline.
-- Down:
--   DROP INDEX IF EXISTS teams_watched_resources_delegated_member_uq;
--   DROP INDEX IF EXISTS teams_watched_resources_app_container_uq;
--   ALTER TABLE teams_watched_resources
--     ADD CONSTRAINT teams_watched_resources_account_id_kind_team_id_channel_id__key
--     UNIQUE (account_id, kind, team_id, channel_id, chat_id);

ALTER TABLE teams_watched_resources
    DROP CONSTRAINT IF EXISTS teams_watched_resources_account_id_kind_team_id_channel_id_chat_id_key;
ALTER TABLE teams_watched_resources
    DROP CONSTRAINT IF EXISTS teams_watched_resources_account_id_kind_team_id_channel_id__key;

CREATE UNIQUE INDEX IF NOT EXISTS teams_watched_resources_delegated_member_uq
    ON teams_watched_resources (account_id, kind, team_id, channel_id, chat_id, user_id)
    WHERE auth_mode = 'delegated';

CREATE UNIQUE INDEX IF NOT EXISTS teams_watched_resources_app_container_uq
    ON teams_watched_resources (account_id, kind, team_id, channel_id, chat_id)
    WHERE auth_mode = 'app';
