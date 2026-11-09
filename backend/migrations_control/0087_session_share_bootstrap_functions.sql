-- Migration: 0087_session_share_bootstrap_functions
-- Description: Third (and, for the route lane, final) batch of RLS bootstrap
--              primitives — the coding-session-share counterparts of 0086's
--              meeting-share pair. Same contract, same reasoning, same
--              discipline as 0084/0086: each function below is a narrow
--              SECURITY DEFINER wrapper answering exactly one "which account
--              (or graph) owns this row?" question for a call site that must
--              DISCOVER the owning account BEFORE it can open a
--              `begin_account_scoped` transaction at all. Both return ONLY a
--              bare account_id / graph_id uuid — never a token, never session
--              metadata, never coral content — so neither can be used to
--              exfiltrate protected content, only to bootstrap a scope
--              decision. See 0084's header for the full model (and why these
--              must NOT be wrapped in `begin_account_scoped` themselves).
--
--              The two call sites these unblock, both in
--              `routes/session_shares.rs`:
--
--              1. `resolve_session_share_account_id` — the PUBLIC viewer
--                 `GET /v1/shared/sessions/{token}` is an ANONYMOUS route on
--                 the `open` router: there is no session, no `UserContext`,
--                 no account of any kind to scope from — only an opaque
--                 capability token. This is the strictest form of the
--                 chicken-and-egg 0084's header describes, since unlike the
--                 authed handlers there is not even a caller account to fall
--                 back on. ACTIVE (non-revoked) shares only, mirroring
--                 `resolve_meeting_share_account_id` exactly: a revoked or
--                 unknown token resolves to NULL and the route returns its
--                 generic 404, so the two stay indistinguishable to a caller
--                 — the security property that route's doc comment promises.
--
--              2. `resolve_session_share_graph_id` — `revoke_session_share`
--                 receives a bare share id and its IDOR guard is graph
--                 REACHABILITY (deliberately multi-account, ADR-0007 Model B
--                 — see 0084's `resolve_reachable_graphs` rationale), so the
--                 scoping account must be the share's OWNING graph's account
--                 (resolved onward via `resolve_graph_account_id`), not the
--                 caller's session account. Matches ANY share, active or
--                 revoked — parity with the previous unscoped
--                 `session_share_graph_id` lookup, so revoking an
--                 already-revoked share still no-ops rather than 404ing.
--                 Same active/any split 0086 made for the meeting pair, for
--                 the same reason.
--
--              Both are CONSUMER/CONTROL-plane only (registered in
--              `scripts/check_schema_parity.sh`'s CONTROL_FUNCTIONS, like
--              0084's `resolve_oauth_account_id` and all of 0086's): the
--              routes that call them resolve their store/pool through
--              `Tenant::legacy` (the public viewer) or the consumer pool,
--              and an org-plane request takes the pre-existing unscoped path
--              on its own whole-database-isolated tenant pool. There is no
--              org-tenant counterpart to pair them into
--              `migrations_tenant/`.
--
--              **This migration is INERT** pre-OPS-4 for the same reason
--              0084/0086 were: the app still connects as the owner/BYPASSRLS
--              role, so swapping raw-table lookups for these functions is a
--              pure query-shape change with byte-identical results today.
--
-- Depends on: 0001_init (graphs), 0077_coding_session_shares (coding_session_shares),
--             0083_consumer_rls (creates `coralswarm_app`),
--             0084_bootstrap_scope_functions (pattern + grants precedent),
--             0086_webhook_share_bootstrap_functions (the meeting-share pair
--             these mirror)
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_session_share_account_id(text) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_session_share_graph_id(uuid) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_session_share_account_id(text);
--   DROP FUNCTION IF EXISTS resolve_session_share_graph_id(uuid);

-- ── 1. Coding-session share token -> owning account (ACTIVE shares only) ─────
-- Mirrors `resolve_meeting_share_account_id` (0086) exactly, retargeted at
-- `coding_session_shares`. Revoked/unknown tokens resolve to NULL.
CREATE OR REPLACE FUNCTION resolve_session_share_account_id(p_token text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.account_id
    FROM coding_session_shares css
    JOIN graphs g ON g.id = css.graph_id
    WHERE css.token = p_token
      AND css.revoked_at IS NULL
    LIMIT 1
$$;

-- ── 2. Coding-session share id -> graph_id (ANY share, active or revoked) ────
-- Mirrors `resolve_meeting_share_graph_id` (0086). Deliberately NOT filtered
-- on `revoked_at`: `revoke_session_share`'s pre-conversion lookup wasn't
-- either, and filtering here would turn a double-revoke into a 404.
CREATE OR REPLACE FUNCTION resolve_session_share_graph_id(p_share_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT graph_id
    FROM coding_session_shares
    WHERE id = p_share_id
    LIMIT 1
$$;

-- ── Grants ──────────────────────────────────────────────────────────────────
-- `coralswarm_app` is the least-privilege role OPS-4 cuts the app over to;
-- CURRENT_USER keeps the migration's own owner able to call them (and keeps
-- local/dev runs working before the cutover). Same grant shape as 0084/0086.
GRANT EXECUTE ON FUNCTION resolve_session_share_account_id(text) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_session_share_graph_id(uuid) TO coralswarm_app, CURRENT_USER;
