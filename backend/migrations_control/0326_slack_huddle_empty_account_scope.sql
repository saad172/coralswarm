-- Fix-forward for 0287; paired with tenant 0158. Keep the existing checksum intact.
-- A transaction-local custom GUC can be an empty string after COMMIT/ROLLBACK
-- or RESET. Missing and empty scope must both deny, not fail UUID parsing.
-- Nonempty malformed values still fail; valid/foreign account semantics are unchanged.
SET LOCAL lock_timeout = '3s';
ALTER POLICY account_isolation ON public.slack_huddle_consents
    USING (account_id = NULLIF(current_setting('app.account_id', true), '')::uuid)
    WITH CHECK (account_id = NULLIF(current_setting('app.account_id', true), '')::uuid);
