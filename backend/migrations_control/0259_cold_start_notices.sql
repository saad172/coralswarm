-- Migration: 0259_cold_start_notices
-- Numbering: this file was authored as 0258 in parallel with
--   0258_chat_cold_start_notices. That table kept 0258; this one moved.
-- Description: Phase G item 25. One row per container for the in-channel
--   "connect your account" fallback. Email is the normal invite. This post
--   runs only when nobody in the account can be reached by email, at most
--   once a day, and at most five times in a rolling 30 days. The first
--   member connect retires it. No message content.
--
--   Control-only, no RLS — same bookkeeping posture as
--   slack_unmatched_email_notices. No tenant twin.
--
-- Depends on: 0257_org_install_capture_grace.
-- Down:
--   DROP TABLE IF EXISTS cold_start_notices;

CREATE TABLE cold_start_notices (
    provider          TEXT        NOT NULL,
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    container_id      TEXT        NOT NULL,
    last_posted_at    TIMESTAMPTZ,
    posts_in_window   INTEGER     NOT NULL DEFAULT 0,
    window_started_at TIMESTAMPTZ,
    PRIMARY KEY (provider, account_id, container_id)
);
