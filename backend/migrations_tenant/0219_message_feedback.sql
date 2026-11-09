CREATE TABLE IF NOT EXISTS message_feedback (
    message_id UUID        NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
    user_id    UUID        NOT NULL,
    account_id UUID        NOT NULL,
    vote       SMALLINT    NOT NULL CHECK (vote IN (-1, 1)),
    reasons    TEXT[]      NOT NULL DEFAULT '{}'
               CHECK (reasons <@ ARRAY['wrong', 'missing_source', 'outdated', 'other']::text[]),
    note       TEXT        CHECK (char_length(note) <= 2000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, user_id)
);

ALTER TABLE message_feedback ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON message_feedback;
CREATE POLICY member_ocean_isolation ON message_feedback
    USING (message_id IN (
        SELECT m.id FROM conversation_messages m
        JOIN conversations c ON c.id = m.conversation_id
        WHERE c.graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

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
