-- Migration: 0228_slack_unmatched_email_notices_unsubscribe
-- Description: One-click unsubscribe for the unmatched-member nudge email
--   (chat-per-user plan PR 6 follow-up, `services::unmatched_recipient_
--   email`). Adds the two columns the suppression list needs on
--   `slack_unmatched_email_notices` (migration 0227, control-plane only) and the
--   ONE control-plane SECURITY DEFINER lookup the public unsubscribe route
--   needs to act on a bare token with no account context at all.
--
--   ── Why now, not at 0227 ──
--   0227 shipped the SEND path only, deliberately: `SLACK_UNMATCHED_EMAIL_
--   ENABLED` defaults OFF specifically because that email had no opt-out
--   (see .env.example's own header on the flag — "no List-Unsubscribe
--   header, no unsubscribe link, and no suppression list to honour one
--   with"). This migration is what makes arming the flag DEFENSIBLE, not a
--   reason to arm it — the flag and its default are untouched here.
--
--   ── `unsubscribe_token`: a random, unguessable per-row secret, NOT an
--   HMAC ──
--   An HMAC(email) would let anyone who ever learns the server-side secret
--   (or a single leaked notice, if the scheme also folded in something
--   guessable) compute every OTHER recipient's unsubscribe URL offline, and
--   rotating a compromised secret would need a coordinated resend of every
--   outstanding link. A random 32-byte value stored ON THE ROW has neither
--   problem: no secret to manage or rotate at all, and revoking one
--   recipient's link (were that ever needed) is a single-row UPDATE, not a
--   fleet-wide key rotation. `UNIQUE` so the unsubscribe route's one query
--   is a single indexed lookup, never a scan. Nullable — existing rows
--   (pre-dating this column) get no token retroactively; `services::
--   unmatched_recipient_email::claim_notice_slot` mints a fresh one on
--   every INSERT and never touches the column again on an UPDATE
--   (`ON CONFLICT ... DO UPDATE` simply omits it from its `SET` list), so a
--   row's token — once it exists — is stable for the row's whole life:
--   an unsubscribe link already delivered to an inbox keeps working even
--   after a later cooldown-renewal claim.
--
--   ── `suppressed_at` ──
--   NULL means "not suppressed" (the common case, and the default for
--   every pre-existing row). Set once, by the unsubscribe route, and never
--   cleared — there is no re-subscribe path, matching the one-way
--   semantics of every other opt-out list in the product.
--   `claim_notice_slot`'s own statement now folds `AND suppressed_at IS
--   NULL` into its EXISTING `ON CONFLICT ... DO UPDATE ... WHERE` clause
--   (see that function's own doc comment) rather than adding a second
--   query, so a suppression can never race a claim: Postgres evaluates one
--   statement's `WHERE` against one row version, there is no window
--   between "check suppressed_at" and "claim the slot" for a concurrent
--   unsubscribe to land in.
--
--   ── `resolve_unmatched_email_notice_account_id`: control-plane, and that
--   is now COMPLETE, not a limitation ──
--   Same bootstrap problem `0086_webhook_share_bootstrap_functions` solved
--   for `routes::shares::public_share`: an unauthenticated caller arrives
--   holding nothing but an opaque token, so the owning account must be
--   DISCOVERED before any `begin_tenant_system_scoped` scope can be opened
--   at all. This function is that discovery step, narrowed to return a
--   bare `account_id` (never a content column) — same posture as 0086's
--   own resolvers, and the actual suppression UPDATE still runs inside a
--   real `begin_tenant_system_scoped(control_pool, account_id)` transaction
--   in Rust (`services::unmatched_recipient_email::suppress_by_token`),
--   preserving this table's own invariant, stated in 0227's header, that
--   only the tenant SYSTEM actor ever writes it.
--
--   An earlier version of this migration's header claimed this function
--   "can only ever resolve a row that lives in the SAME physical database
--   this function is installed in", the same limitation `routes::shares::
--   public_share` genuinely has for meeting-share tokens minted in an org
--   tenant. That comparison was wrong for THIS table: `meeting_shares` is
--   real ocean content, sharded per tenant on purpose; `slack_unmatched_
--   email_notices` is send bookkeeping with no ocean anchor at all (0227's
--   header), and 0227 now has no tenant-tree twin — every recipient's row,
--   personal account or org account alike, is written to THIS database,
--   unconditionally, by `workers::slack_unmatched_email::process_job` (see
--   that worker's own module doc). So this function resolves every token
--   this table can ever hold; there is no cross-plane gap left to flag.
--   Registered in `scripts/check_schema_parity.sh`'s CONTROL_FUNCTIONS
--   (never paired into `migrations_tenant`): a tenant database has no
--   unauthenticated public route to run it FROM, so a tenant-tree twin
--   would be genuinely dead code, not merely unused.
--
-- Depends on: 0227_slack_unmatched_email_notices (the table),
--             0083_consumer_rls (creates `coralswarm_app`),
--             0086_webhook_share_bootstrap_functions (the SECURITY DEFINER
--             bootstrap-resolver pattern this follows exactly).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_unmatched_email_notice_account_id(text) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_unmatched_email_notice_account_id(text);
--   ALTER TABLE slack_unmatched_email_notices DROP COLUMN IF EXISTS suppressed_at;
--   ALTER TABLE slack_unmatched_email_notices DROP COLUMN IF EXISTS unsubscribe_token;

ALTER TABLE slack_unmatched_email_notices
    ADD COLUMN unsubscribe_token TEXT UNIQUE,
    ADD COLUMN suppressed_at TIMESTAMPTZ;

-- Bootstrap resolver: bare account_id only, exactly 0086's shape and
-- exactly 0086's reasoning (see this migration's own header above).
CREATE OR REPLACE FUNCTION resolve_unmatched_email_notice_account_id(p_token text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM slack_unmatched_email_notices
    WHERE unsubscribe_token = p_token
    LIMIT 1
$$;

-- Same load-bearing REVOKE-then-GRANT as 0084/0086 (functions default to
-- PUBLIC EXECUTE; this bypasses RLS by design, so PUBLIC must be closed).
REVOKE EXECUTE ON FUNCTION resolve_unmatched_email_notice_account_id(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_unmatched_email_notice_account_id(text) TO coralswarm_app, CURRENT_USER;
