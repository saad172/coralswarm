-- Migration 0035: Outlook thread auto-capture (CS-117)
--
-- Backs "ingest this thread, and keep it current even when someone replies".
-- Two tables:
--
--   * outlook_watched_conversations — the set of Outlook conversations a user
--     explicitly ingested. Inbound Graph change-notifications are matched
--     against this set: a new message whose conversationId is here gets fetched
--     and ingested; everything else is discarded. This is what keeps the
--     auto-capture *storage*-scoped to opted-in threads even though the Graph
--     subscription itself is mailbox-wide (Graph cannot subscribe per
--     conversation — see ADR-0008).
--
--   * graph_subscriptions — the live Microsoft Graph change-notification
--     subscription(s) backing the watch, one per account+resource (the Inbox).
--     Carries the per-subscription clientState secret (verified on every
--     notification) and the expiry the renewal worker tracks.

CREATE TABLE IF NOT EXISTS outlook_watched_conversations (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Destination ocean for newly-captured replies (the ocean the thread was
    -- first ingested into).
    graph_id        UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    -- The user who opted the thread in. Auto-captured replies are ingested as
    -- this user (reachability + provenance), since notifications carry no caller.
    ingested_by     UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Microsoft Graph conversationId (stable across all messages in a thread).
    conversation_id TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Bumped whenever we (re)ingest a message for this conversation.
    last_ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, conversation_id)
);

-- Notification matching is by (account_id, conversation_id) — the unique index
-- above already serves it. This index serves the renewal/cleanup scans by account.
CREATE INDEX IF NOT EXISTS idx_watched_conversations_account
    ON outlook_watched_conversations (account_id);

CREATE TABLE IF NOT EXISTS graph_subscriptions (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Microsoft's subscription id (globally unique; returned by POST /subscriptions).
    subscription_id TEXT        NOT NULL UNIQUE,
    -- The watched Graph resource, e.g. "me/mailFolders('Inbox')/messages".
    resource        TEXT        NOT NULL,
    -- Secret we generate and hand to Graph at creation; echoed back in every
    -- notification's clientState and verified (constant-time) before we act.
    client_state    TEXT        NOT NULL,
    -- When Microsoft will stop sending notifications (mail subs live < 3 days).
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One subscription per account per watched resource (the Inbox today).
    UNIQUE (account_id, resource)
);

-- Renewal worker scans for subscriptions nearing expiry.
CREATE INDEX IF NOT EXISTS idx_graph_subscriptions_expiry
    ON graph_subscriptions (expires_at);
