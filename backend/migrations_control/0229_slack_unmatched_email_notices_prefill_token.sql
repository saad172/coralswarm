-- Migration: 0229_slack_unmatched_email_notices_prefill_token
-- Description: CodeRabbit round 1 (#1005, Security & Privacy): the nudge
--   email's signup link carried the recipient's address as a plain
--   `?email=` query parameter, which browser history, proxy access logs,
--   analytics, and `Referer` headers can all retain. This migration adds
--   the opaque, DB-backed token the finding asked for.
--
--   ── Why a random column, not an HMAC/JWT ──
--   Same reasoning `0228`'s header already gives for `unsubscribe_token`,
--   restated because it is easy to get wrong here specifically: a signed
--   token (JWT/HMAC) is INTEGRITY-protected, not CONFIDENTIAL — its payload
--   is base64, not encrypted, so embedding the email inside a "signed"
--   token would still leak the address to anyone who captures the URL,
--   just with an extra decode step. That would look fixed while not being
--   fixed. A random, unguessable value stored ON THE ROW carries no
--   information about the email at all; resolving it requires a database
--   lookup only the server can perform.
--
--   ── One more column, not a new table ──
--   Same row, same table, same "send bookkeeping with no ocean anchor"
--   posture `0227`'s header already established. `signup_prefill_token` is
--   independent of `unsubscribe_token` (different purpose, different
--   consumer, no reason to couple their lifetimes) but minted alongside it
--   by the SAME `claim_notice_slot` statement, and self-heals a
--   pre-existing NULL row the identical way `unsubscribe_token` does (see
--   that column's own `COALESCE` reasoning in `0228`'s header — this
--   migration's Rust caller does the same).
--
--   ── The resolver returns a bare account_id, never the email ──
--   Exactly `resolve_unmatched_email_notice_account_id`'s own shape and
--   reasoning (`0228`'s header): a SECURITY DEFINER function bypasses RLS,
--   so it stays narrowed to bookkeeping identifiers, never a content
--   column. The actual email read happens in Rust
--   (`services::unmatched_recipient_email::resolve_prefill_token`) inside a
--   real `begin_tenant_system_scoped(control_pool, account_id)`
--   transaction, the identical two-step shape `suppress_by_token` already
--   uses for the unsubscribe route.
--
-- Depends on: 0227_slack_unmatched_email_notices (the table),
--             0228_slack_unmatched_email_notices_unsubscribe (the
--             `resolve_..._account_id` bootstrap-resolver shape this
--             follows exactly).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_unmatched_email_notice_account_id_by_prefill_token(text) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_unmatched_email_notice_account_id_by_prefill_token(text);
--   ALTER TABLE slack_unmatched_email_notices DROP COLUMN IF EXISTS signup_prefill_token;

ALTER TABLE slack_unmatched_email_notices
    ADD COLUMN signup_prefill_token TEXT UNIQUE;

-- Bootstrap resolver: bare account_id only, exactly 0228's shape.
CREATE OR REPLACE FUNCTION resolve_unmatched_email_notice_account_id_by_prefill_token(p_token text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM slack_unmatched_email_notices
    WHERE signup_prefill_token = p_token
    LIMIT 1
$$;

-- Same load-bearing REVOKE-then-GRANT as 0084/0086/0228 (functions default
-- to PUBLIC EXECUTE; this bypasses RLS by design, so PUBLIC must be closed).
REVOKE EXECUTE ON FUNCTION resolve_unmatched_email_notice_account_id_by_prefill_token(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_unmatched_email_notice_account_id_by_prefill_token(text) TO coralswarm_app, CURRENT_USER;
