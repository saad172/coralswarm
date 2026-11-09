-- Migration: 0086_webhook_share_bootstrap_functions
-- Description: Second batch of RLS bootstrap primitives (org-tenancy Phase B/W
--              conversion follow-up to 0084_bootstrap_scope_functions). Same
--              contract, same reasoning, same discipline as 0084: each
--              function below is a narrow SECURITY DEFINER wrapper answering
--              exactly one "which account (or graph) owns this row?" question
--              for a call site that must DISCOVER the owning account BEFORE it
--              can open a `begin_account_scoped` transaction at all — the
--              chicken-and-egg 0084's header describes. Every function returns
--              ONLY bare account_id/graph_id uuids — never a content column
--              (no tokens, no client_state, no share metadata, no transcript
--              text) — so none can be used to exfiltrate protected content,
--              only to bootstrap a scope decision. See 0084's header for the
--              full model (and why these must NOT be wrapped in
--              `begin_account_scoped` themselves).
--
--              The five call-site families these unblock:
--
--              1. `resolve_slack_integration_account_id` — the Slack message
--                 webhook funnel (`routes/slack_events.rs::ingest_slack_message`)
--                 arrives carrying only a Slack `team_id` and resolves its
--                 account via `slack_integrations.workspace_id` (NOT
--                 `oauth_tokens` — legacy installs through
--                 `routes/slack.rs::oauth_callback` write ONLY
--                 `slack_integrations`, so 0084's `resolve_oauth_account_id`
--                 would silently orphan them). Returns the account id only;
--                 the caller re-reads `workspace_name`/`bot_token` INSIDE the
--                 scoped transaction it opens with that id.
--
--              2. `resolve_graph_subscription_account_id` — the Microsoft
--                 Graph change-notification webhook
--                 (`routes/graph_webhook.rs` via
--                 `connectors/microsoft.rs::subscription_by_id`) and the
--                 subscription-renewal sweep identify work by a bare
--                 provider-issued `subscription_id`; the owning account must
--                 be discovered before the `graph_subscriptions` row
--                 (including its `client_state` validation secret) can be
--                 read under scope.
--
--              3. `resolve_meeting_share_account_id` — the share viewers
--                 (`routes/shares.rs::{public_share,view_share}`) authenticate
--                 by CAPABILITY TOKEN, not by account: ANY holder of an
--                 active link-share token may view that share (cross-account
--                 by design), so the serving scope is the share OWNER's
--                 account, resolvable only from the token itself. Active
--                 (non-revoked) shares only — a revoked/unknown token
--                 resolves to NULL and the caller 404s, indistinguishable
--                 from before.
--
--              4. `resolve_meeting_share_graph_id` — `routes/shares.rs::
--                 revoke_share` receives a bare share id; its IDOR guard is
--                 graph REACHABILITY (deliberately multi-account, Model B —
--                 see 0084's `resolve_reachable_graphs` rationale), so the
--                 scoping account must be the share's OWNING graph's account
--                 (via `resolve_graph_account_id`), not the caller's session
--                 account. Any share (active or revoked) — parity with the
--                 previous unscoped `meeting_share_graph_id` lookup; the
--                 reachability guard still runs before anything is served.
--
--              5. `resolve_expiring_subscriptions` /
--                 `resolve_watched_conversation_accounts` /
--                 `resolve_teams_watched_accounts` — the WORK-SET DISCOVERY
--                 queries the periodic sweeps in
--                 `workers/graph_subscriptions.rs` start from (subscription
--                 renewal; the Outlook and Teams backstop resyncs). A sweep
--                 over "every account that has a watch / a subscription due"
--                 is cross-account by definition — there is no account to
--                 scope it to, it exists to enumerate them. Left as raw table
--                 reads these would post-OPS-4 silently return zero rows and
--                 quietly stop every renewal, so they get the same narrow
--                 `SECURITY DEFINER` treatment as the rest: bare ids only
--                 (`account_id`, plus the provider's own `subscription_id` for
--                 the renewal queue — never `resource`, never `tenant_id`,
--                 and above all never `client_state`). Each id is then
--                 processed under its own `begin_account_scoped` transaction,
--                 so the only thing crossing an account boundary is the
--                 identifier the sweep needs to open a scope with.
--
--              6. `resolve_node_account_id` — the cross-DB promotion service
--                 (`services/promotion.rs::promote_node_to_org`) starts from
--                 a bare `source_node_id` and must find its owning account
--                 (node → graph → account) before any scoped read of the
--                 source rows. The full ownership predicate (personal
--                 account-of-one, `kind = 'personal'`) is re-verified INSIDE
--                 the scoped transaction; this only breaks the bootstrap.
--
--              All five are CONSUMER/CONTROL-plane only (registered in
--              `scripts/check_schema_parity.sh`'s CONTROL_FUNCTIONS, like
--              0084's `resolve_oauth_account_id`): their call sites are the
--              signature-authenticated webhook funnels, the consumer-plane
--              share viewers, and the promotion service's consumer-side
--              source reads — none of which ever runs against a tenant pool
--              (org-plane requests take the pre-existing unscoped path on
--              their own whole-database-isolated pool).
--
--              **This migration is INERT** pre-OPS-4 for the same reason
--              0084 was: the app still connects as the owner/BYPASSRLS role,
--              so swapping raw-table lookups for these functions is a pure
--              query-shape change with byte-identical results today.
--
-- Depends on: 0001_init (accounts, graphs), 0011_nodes (nodes),
--             0015_slack_integrations, 0068_meeting_shares,
--             0036_outlook_workflows-era graph_subscriptions,
--             0083_consumer_rls (creates `coralswarm_app`),
--             0084_bootstrap_scope_functions (pattern + grants precedent)
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_slack_integration_account_id(text) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_graph_subscription_account_id(text) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_meeting_share_account_id(text) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_meeting_share_graph_id(uuid) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_node_account_id(uuid) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_expiring_subscriptions(timestamptz) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_watched_conversation_accounts() FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_teams_watched_accounts() FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_expiring_subscriptions(timestamptz);
--   DROP FUNCTION IF EXISTS resolve_watched_conversation_accounts();
--   DROP FUNCTION IF EXISTS resolve_teams_watched_accounts();
--   DROP FUNCTION IF EXISTS resolve_slack_integration_account_id(text);
--   DROP FUNCTION IF EXISTS resolve_graph_subscription_account_id(text);
--   DROP FUNCTION IF EXISTS resolve_meeting_share_account_id(text);
--   DROP FUNCTION IF EXISTS resolve_meeting_share_graph_id(uuid);
--   DROP FUNCTION IF EXISTS resolve_node_account_id(uuid);

-- ── 1. Slack workspace_id -> account_id (slack_integrations bootstrap) ───────
-- Mirrors the previous `SELECT account_id ... FROM slack_integrations WHERE
-- workspace_id = $1 LIMIT 1` webhook lookup, minus every content column.
CREATE OR REPLACE FUNCTION resolve_slack_integration_account_id(p_workspace_id text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM slack_integrations
    WHERE workspace_id = p_workspace_id
    LIMIT 1
$$;

-- ── 2. Graph subscription_id -> account_id (webhook/renewal bootstrap) ───────
-- Never returns `client_state` (the notification-validation secret) — the
-- caller reads that inside the scoped transaction it opens with this id.
CREATE OR REPLACE FUNCTION resolve_graph_subscription_account_id(p_subscription_id text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM graph_subscriptions
    WHERE subscription_id = p_subscription_id
    LIMIT 1
$$;

-- ── 3. Meeting-share token -> owning account (capability-token bootstrap) ────
-- ACTIVE shares only: a revoked token must stay indistinguishable from an
-- unknown one (routes/shares.rs's generic-404 contract), and neither may
-- resolve a scope. Anchored through the share's graph, the same chain the
-- table's own RLS policy uses.
CREATE OR REPLACE FUNCTION resolve_meeting_share_account_id(p_token text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.account_id
    FROM meeting_shares ms
    JOIN graphs g ON g.id = ms.graph_id
    WHERE ms.token = p_token
      AND ms.revoked_at IS NULL
    LIMIT 1
$$;

-- ── 4. Meeting-share id -> graph_id (revoke-path bootstrap) ──────────────────
-- Any share, active or revoked (revoking twice must keep working exactly as
-- it did unscoped). Returns the graph id only; the caller derives the scope
-- account via 0084's `resolve_graph_account_id` and re-verifies reachability
-- before touching the row.
CREATE OR REPLACE FUNCTION resolve_meeting_share_graph_id(p_share_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT graph_id
    FROM meeting_shares
    WHERE id = p_share_id
    LIMIT 1
$$;

-- ── 5. node_id -> account_id (promotion-source bootstrap) ────────────────────
-- Same shape as 0084's `resolve_atoms_account_id`, for a single node.
CREATE OR REPLACE FUNCTION resolve_node_account_id(p_node_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.account_id
    FROM nodes n
    JOIN graphs g ON g.id = n.graph_id
    WHERE n.node_id = p_node_id
$$;

-- ── 6. Sweep work-set discovery (cross-account by definition) ───────────────
-- Subscription-renewal queue: `(subscription_id, account_id)` ONLY. The
-- renewal worker re-reads `resource`/`tenant_id` per subscription inside the
-- `begin_account_scoped` transaction it opens with the account_id returned
-- here; `client_state` is never exposed by any of these.
CREATE OR REPLACE FUNCTION resolve_expiring_subscriptions(p_cutoff timestamptz)
RETURNS TABLE (subscription_id text, account_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT gs.subscription_id, gs.account_id
    FROM graph_subscriptions gs
    WHERE gs.expires_at < p_cutoff
$$;

-- Outlook backstop-resync work set: distinct owning accounts, nothing else.
CREATE OR REPLACE FUNCTION resolve_watched_conversation_accounts()
RETURNS TABLE (account_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT owc.account_id FROM outlook_watched_conversations owc
$$;

-- Teams backstop-resync work set: distinct owning accounts, nothing else.
CREATE OR REPLACE FUNCTION resolve_teams_watched_accounts()
RETURNS TABLE (account_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT twr.account_id FROM teams_watched_resources twr
$$;

-- ── Grants ────────────────────────────────────────────────────────────────
-- Same load-bearing REVOKE-then-GRANT as 0084 (functions default to PUBLIC
-- EXECUTE; these bypass RLS by design, so PUBLIC must be closed).
REVOKE EXECUTE ON FUNCTION resolve_slack_integration_account_id(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_graph_subscription_account_id(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_meeting_share_account_id(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_meeting_share_graph_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_node_account_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_expiring_subscriptions(timestamptz) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_watched_conversation_accounts() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_teams_watched_accounts() FROM PUBLIC;

GRANT EXECUTE ON FUNCTION resolve_expiring_subscriptions(timestamptz) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_watched_conversation_accounts() TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_teams_watched_accounts() TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_slack_integration_account_id(text) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_graph_subscription_account_id(text) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_meeting_share_account_id(text) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_meeting_share_graph_id(uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_node_account_id(uuid) TO coralswarm_app, CURRENT_USER;
