-- Migration 0037: Microsoft Teams ingest opt-in targets (CS-119)
--
-- The set of Teams channels and chats a user has opted into ingesting. Mirrors
-- the per-thread/per-channel opt-in model used for Slack and Outlook: only what
-- the user explicitly chose is read and stored — never the whole Teams estate.
--
-- Backfill (delegated read of the opted-in resource) runs on opt-in; live
-- capture via Graph change notifications (CS-119 phase 2) reuses the
-- `graph_subscriptions` table + `/v1/webhooks/graph` endpoint and matches an
-- inbound message's channel/chat id against this set.

CREATE TABLE IF NOT EXISTS teams_watched_resources (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Owner; ingested messages are attributed to this user (provenance +
    -- reachability), like `ingested_by` on a watched email conversation.
    user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Destination ocean (defaults to the owner's primary ocean).
    graph_id        UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    -- 'channel' or 'chat'.
    kind            TEXT        NOT NULL CHECK (kind IN ('channel', 'chat')),
    -- For channels: the team + channel ids. For chats: chat_id. Unused ids are
    -- the empty string (not NULL) so the unique key below dedupes correctly —
    -- NULLs would be treated as distinct.
    team_id         TEXT        NOT NULL DEFAULT '',
    channel_id      TEXT        NOT NULL DEFAULT '',
    chat_id         TEXT        NOT NULL DEFAULT '',
    -- Human label for the picker / management UI (team » channel, or chat topic).
    display_name    TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A channel is identified by (team_id, channel_id); a chat by chat_id.
    UNIQUE (account_id, kind, team_id, channel_id, chat_id)
);

-- Live-notification matching looks up by channel_id or chat_id within an account.
CREATE INDEX IF NOT EXISTS idx_teams_watched_channel
    ON teams_watched_resources (account_id, channel_id);
CREATE INDEX IF NOT EXISTS idx_teams_watched_chat
    ON teams_watched_resources (account_id, chat_id);
