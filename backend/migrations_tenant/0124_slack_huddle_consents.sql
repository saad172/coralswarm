-- Metadata-only per-huddle disclosure and confirmation. Content is never stored here.
-- Paired with control migration 0287. Separate from the terminal capture ledger.
CREATE TABLE slack_huddle_consents (
    id UUID PRIMARY KEY,
    -- accounts is control-only; tenant offboarding removes the database.
    account_id UUID NOT NULL,
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    huddle_thread_ts TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    non_attendee_count INTEGER NOT NULL CHECK (non_attendee_count >= 0),
    capture_payload JSONB NOT NULL,
    prompt_ts TEXT,
    confirmed_slack_user_id TEXT,
    confirmed_user_id UUID,
    confirmed_at TIMESTAMPTZ,
    UNIQUE (account_id, workspace_id, channel_id, huddle_thread_ts)
);
ALTER TABLE slack_huddle_consents ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON slack_huddle_consents
    USING (account_id = current_setting('app.account_id', true)::uuid)
    WITH CHECK (account_id = current_setting('app.account_id', true)::uuid);
