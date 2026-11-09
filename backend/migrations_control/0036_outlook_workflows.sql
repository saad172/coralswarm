-- Migration 0036: Outlook ingest workflows (CS-118)
--
-- Per-user "auto-ingest" rules: when a new message matches a user's filter, its
-- whole conversation is auto-watched and ingested (same path as a manual
-- "ingest this thread" opt-in, just rule-triggered). Live matching runs in the
-- change-notification webhook; rule creation also kicks off a bounded backfill
-- of recent matching mail. See ADR-0008 (D6).
--
-- Rules are owned by a user and scoped to the account's connected mailbox. The
-- `filter` JSONB holds the match criteria (from address/domain, subject
-- keywords, recipient address/domain, Outlook category, folder, has-attachment);
-- semantics are AND across populated criteria, OR within each.

CREATE TABLE IF NOT EXISTS outlook_workflows (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id  UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Owner; auto-ingested mail is attributed to this user (provenance +
    -- reachability), exactly like the `ingested_by` on a watched conversation.
    user_id     UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Destination ocean for matches (defaults to the owner's primary ocean).
    graph_id    UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    enabled     BOOLEAN     NOT NULL DEFAULT TRUE,
    -- Match criteria; shape defined by routes::workflows::WorkflowFilter.
    filter      JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The webhook evaluates all enabled rules for an account on each notification.
CREATE INDEX IF NOT EXISTS idx_outlook_workflows_account_enabled
    ON outlook_workflows (account_id)
    WHERE enabled;

-- Listing a user's own rules.
CREATE INDEX IF NOT EXISTS idx_outlook_workflows_user
    ON outlook_workflows (account_id, user_id);
