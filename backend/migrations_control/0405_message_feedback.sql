CREATE TABLE IF NOT EXISTS message_feedback (
    message_id UUID        NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
    user_id    UUID        NOT NULL,
    account_id UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    vote       SMALLINT    NOT NULL CHECK (vote IN (-1, 1)),
    reasons    TEXT[]      NOT NULL DEFAULT '{}'
               CHECK (reasons <@ ARRAY['wrong', 'missing_source', 'outdated', 'other']::text[]),
    note       TEXT        CHECK (char_length(note) <= 2000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, user_id)
);

ALTER TABLE message_feedback ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON message_feedback;
CREATE POLICY account_isolation ON message_feedback
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);

DROP POLICY IF EXISTS message_feedback_own_insert ON message_feedback;
CREATE POLICY message_feedback_own_insert ON message_feedback AS RESTRICTIVE FOR INSERT
    WITH CHECK (user_id = NULLIF(current_setting('app.user_id', true), '')::uuid);
DROP POLICY IF EXISTS message_feedback_own_update ON message_feedback;
CREATE POLICY message_feedback_own_update ON message_feedback AS RESTRICTIVE FOR UPDATE
    USING (user_id = NULLIF(current_setting('app.user_id', true), '')::uuid)
    WITH CHECK (user_id = NULLIF(current_setting('app.user_id', true), '')::uuid);
DROP POLICY IF EXISTS message_feedback_own_delete ON message_feedback;
CREATE POLICY message_feedback_own_delete ON message_feedback AS RESTRICTIVE FOR DELETE
    USING (user_id = NULLIF(current_setting('app.user_id', true), '')::uuid);
