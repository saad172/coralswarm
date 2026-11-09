-- Outlook Everything resolves a personal graph for consumer accounts and a
-- member graph for org accounts. The original policy admitted only the latter.
-- Keep graph/account/owner attribution and the archived-graph refusal intact.
DROP POLICY account_isolation ON outlook_everything_mode;
CREATE POLICY account_isolation ON outlook_everything_mode
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND EXISTS (
            SELECT 1 FROM graphs
            WHERE graphs.id = outlook_everything_mode.graph_id
              AND graphs.account_id = outlook_everything_mode.account_id
              AND graphs.archived_at IS NULL
              AND (
                  (graphs.kind = 'member' AND graphs.owner_user_id = outlook_everything_mode.owner_user_id)
                  OR (graphs.kind = 'personal' AND graphs.owner_user_id IS NULL
                      AND EXISTS (SELECT 1 FROM accounts
                                  WHERE accounts.id = graphs.account_id AND accounts.clerk_org_id IS NULL
                                    AND accounts.owner_user_id = outlook_everything_mode.owner_user_id))
              )
        )
    );
