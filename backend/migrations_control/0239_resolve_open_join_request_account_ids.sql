-- Migration: 0239_resolve_open_join_request_account_ids
-- Description: CS-437 CodeRabbit follow-up — the ONE control-plane
--   SECURITY DEFINER resolver `GET /v1/me/join-requests` needs to list a
--   signed-in user's open join requests across organizations.
--
--   `org_join_requests` (migration 0238) carries a strict
--   `account_isolation` policy keyed on `current_setting('app.account_id')`.
--   Every single-account reader/writer in `services::org_join_requests`
--   opens `tenancy::scope::begin_account_scoped` for that reason.
--   `list_open_for_user` cannot: the requester is, by definition, not a
--   member of the orgs they asked to join, so no one `app.account_id` can
--   expose every row. Same bootstrap problem 0084/0086 solved for
--   graph/share lookups, and 0228 solved for the unmatched-email
--   unsubscribe token: discover WHICH accounts to scope to, then do the
--   real read inside a scoped transaction.
--
--   This function returns bare `account_id`s only (never a content
--   column) — identical posture to `resolve_atoms_account_id` /
--   `resolve_unmatched_email_notice_account_id`. The subsequent SELECT of
--   `id`/`clerk_org_id`/`requested_at` still runs inside
--   `begin_account_scoped` in Rust.
--
--   Control-only by NECESSITY: `org_join_requests` is in
--   `scripts/check_schema_parity.sh`'s CONTROL_TABLES and has no
--   tenant-tree twin (0238's own header). Registered in CONTROL_FUNCTIONS
--   in this same PR.
--
-- Depends on: 0238_org_join_requests (the table + RLS policy),
--             0083_consumer_rls (creates `coralswarm_app`),
--             0084_bootstrap_scope_functions (the SECURITY DEFINER
--             resolver pattern this follows).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_open_join_request_account_ids(uuid) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_open_join_request_account_ids(uuid);

CREATE OR REPLACE FUNCTION resolve_open_join_request_account_ids(p_user_id uuid)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM org_join_requests
    WHERE user_id = p_user_id AND decision IS NULL
$$;

-- Same load-bearing REVOKE-then-GRANT as 0084/0086 (functions default to
-- PUBLIC EXECUTE; this bypasses RLS by design, so PUBLIC must be closed).
REVOKE EXECUTE ON FUNCTION resolve_open_join_request_account_ids(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_open_join_request_account_ids(uuid) TO coralswarm_app, CURRENT_USER;
