-- Migration: 0256_slack_member_channels
-- Description: CS-459 / Phase G wave 3 item 46b (S8) — which Slack channels
--   a connected member can read, learned from `users.conversations` on that
--   member's own `slack_user` (xoxp) token. Routing index only: no message
--   content. A workspace message event is a doorbell; ingest targets are
--   the rows in this table, and each member's history is fetched (and their
--   cursor advanced) with their own token.
--
--   Control-plane only, no RLS — same routing-index posture as
--   `teams_container_watchers` (0255) and `connector_installs`. A member's
--   `history_cursor` / `backfill_complete` are per row, so refreshing one
--   member never resets another's cursor.
--
-- Depends on: 0255_teams_container_doorbell.
-- Down:
--   DROP TABLE IF EXISTS slack_member_channels;

CREATE TABLE slack_member_channels (
    account_id         UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id            UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    workspace_id       TEXT        NOT NULL,
    channel_id         TEXT        NOT NULL,
    history_cursor     TEXT,
    backfill_complete  BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, user_id, workspace_id, channel_id)
);

CREATE INDEX slack_member_channels_channel_idx
    ON slack_member_channels (workspace_id, channel_id);
