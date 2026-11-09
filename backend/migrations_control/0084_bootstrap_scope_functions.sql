-- Migration: 0084_bootstrap_scope_functions
-- Description: RLS bootstrap primitives (org-tenancy Phase S, follow-up to
--              migration 0083_consumer_rls). `tenancy::scope::begin_account_scoped`
--              (`SET LOCAL app.account_id`) has to be called BEFORE any query
--              against one of the 36 RLS-protected consumer-plane tables — but
--              several existing call sites need to DISCOVER an account_id by
--              querying one of those very tables first (e.g. "which account
--              owns this graph_id?"), a chicken-and-egg problem: you cannot
--              scope a connection to an account_id you don't know yet.
--
--              Each function below is a narrow `SECURITY DEFINER` wrapper
--              that answers exactly one "which account owns this row?"
--              question and returns ONLY account_id/graph_id columns — never
--              any content column (no atom text, no oauth tokens, no graph
--              name in the account-resolution functions) — so they cannot be
--              used to exfiltrate protected content, only to bootstrap a
--              scope decision. `SECURITY DEFINER` makes them run with the
--              privileges of their OWNER (not the caller), so they see every
--              account's rows regardless of the caller's own `app.account_id`
--              (or lack of one) — this is the ONE deliberate, narrow, audited
--              exception to "every query needs a prior scope" in this
--              codebase. See `tests/bootstrap_scope_pg.rs` for the proof, and
--              the per-function comment below for why each one may NOT be
--              wrapped in `begin_account_scoped`/RLS without reintroducing
--              the deadlock it exists to break.
--
--              Every function pins `SET search_path = public, pg_temp`
--              (defense in depth standard for `SECURITY DEFINER`: without it,
--              a caller able to manipulate the session's `search_path` could
--              shadow an unqualified table/function reference with one of
--              their own — not exploitable today given `coralswarm_app` has
--              no `CREATE` on `public`, but cheap and correct to pin anyway,
--              matching this migration's least-privilege discipline).
--
--              Two of the five ONLY make sense on the consumer/control plane
--              and are NOT paired into `migrations_tenant` (registered in
--              `scripts/check_schema_parity.sh`'s `CONTROL_FUNCTIONS` so the
--              parity job doesn't flag the asymmetry):
--                - `resolve_oauth_account_id`: the Slack webhook call sites
--                  that use it (`routes/slack_events.rs`,
--                  `routes/slack_interactivity.rs`) always read `state.db`,
--                  the single shared consumer pool — never a tenant pool.
--                - `resolve_reachable_graphs` / `resolve_graph_reachable_by_user`:
--                  re-implement `list_reachable_graphs`/`graph_reachable_by_user`'s
--                  `account_members`-joined predicate, and `account_members`
--                  is a control-only table (no row-per-org-tenant-DB
--                  membership concept there) — these two could not exist in
--                  `migrations_tenant` even if we wanted them to.
--              The other two (`resolve_graph_account_id`, `resolve_atoms_account_id`)
--              back `Store::graph_account_id` and
--              `workers::embed::resolve_dek_for_atoms`, BOTH of which are
--              called against either plane's pool today (`graphs`/`atoms` are
--              shared tenant-tree tables) — see the paired migration
--              `migrations_tenant/0003_bootstrap_scope_functions.sql`, whose
--              body text is kept byte-identical to the two here so
--              `check_schema_parity.sh`'s catalog diff stays clean.
--
--              **This migration is INERT** — same "lands inert" discipline as
--              0083 itself: nothing calls these functions with `SET LOCAL`
--              removed from any existing path, because nothing calls them at
--              all yet outside `tests/bootstrap_scope_pg.rs` until THIS PR's
--              own Rust-side swap (the SQL bodies of `Store::graph_account_id`,
--              `resolve_dek_for_atoms`, the three Slack account-resolution
--              call sites, and `routes/sync.rs`'s graph lookup, changed to
--              call these functions instead of querying the raw tables
--              directly — no RLS enforcement is active yet, since the app
--              still connects as the owner/BYPASSRLS role pre-OPS-4, so this
--              is a pure query-shape swap with byte-identical results today).
--
--              **`resolve_reachable_graphs`/`resolve_graph_reachable_by_user`
--              are additionally flagged for founder review**: `graphs.account_id
--              IN (SELECT account_id FROM account_members WHERE user_id = $1)`
--              is deliberately MULTI-account (a user can belong to several
--              `accounts` rows and see across all of them in one query, per
--              `middleware/auth.rs`'s documented "Model B") — incompatible
--              with single-`account_id` RLS scoping as designed. The decision
--              (already made, not this migration's to revisit) is: give this
--              query a PERMANENT `SECURITY DEFINER` carve-out that internally
--              re-implements the identical predicate, rather than RLS-scoping
--              it or changing product UX. A bug in the predicate is still
--              caught by review/tests; it is just never double-enforced by a
--              Postgres policy the way the other 36 tables are.
--
-- Depends on: 0001_init (accounts, account_members, graphs), 0003_atoms
--             (atoms), 0021_oauth_tokens, 0022_teams (teams, team_members),
--             0083_consumer_rls (creates the `coralswarm_app` role these
--             functions are granted to)
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_graph_account_id(uuid) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_atoms_account_id(uuid[]) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_oauth_account_id(text, text) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_graph_reachable_by_user(uuid, uuid) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_graph_account_id(uuid);
--   DROP FUNCTION IF EXISTS resolve_atoms_account_id(uuid[]);
--   DROP FUNCTION IF EXISTS resolve_oauth_account_id(text, text);
--   DROP FUNCTION IF EXISTS resolve_reachable_graphs(uuid);
--   DROP FUNCTION IF EXISTS resolve_graph_reachable_by_user(uuid, uuid);

-- ── 1. graph_id -> account_id (backs Store::graph_account_id) ───────────────
-- Bootstrap carve-out: DO NOT wrap this in `begin_account_scoped`/RLS — its
-- entire purpose is to answer "which account owns this graph?" BEFORE the
-- caller knows the account_id needed to scope a connection in the first
-- place. Wrapping it would reintroduce the exact chicken-and-egg deadlock
-- this migration exists to break. Returns only a bare uuid — never any other
-- `graphs` column — so it cannot be used to read graph content.
CREATE OR REPLACE FUNCTION resolve_graph_account_id(p_graph_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id FROM graphs WHERE id = p_graph_id
$$;

-- ── 2. atom_ids[] -> distinct account_id(s) (backs resolve_dek_for_atoms) ───
-- Same bootstrap carve-out reasoning as (1): resolving the DEK for a batch of
-- atom_ids requires knowing their account BEFORE any scoped connection can be
-- opened. Returns `SETOF uuid` (not a single scalar) so the caller keeps its
-- existing "exactly one distinct account_id came back, or refuse as
-- ambiguous" invariant check unchanged — this function does not enforce that
-- invariant itself, it only re-implements the identical unscoped lookup.
CREATE OR REPLACE FUNCTION resolve_atoms_account_id(p_atom_ids uuid[])
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT g.account_id
    FROM graphs g
    JOIN atoms at ON at.graph_id = g.id
    WHERE at.atom_id = ANY(p_atom_ids)
$$;

-- ── 3. Slack (provider, workspace_id) -> account_id (webhook bootstrap) ─────
-- Same carve-out reasoning: a Slack event/interactivity webhook arrives
-- carrying only a Slack team_id, and the FIRST thing every handler must do is
-- discover which account that workspace belongs to — there is no account_id
-- to scope a connection with until this resolves. Returns only a bare uuid,
-- never any other `oauth_tokens` column (in particular, never the OAuth
-- token itself), so it cannot be used to exfiltrate integration secrets.
-- Control-plane only: see this migration's header (not paired into
-- `migrations_tenant`, registered in `check_schema_parity.sh`'s
-- `CONTROL_FUNCTIONS` instead).
CREATE OR REPLACE FUNCTION resolve_oauth_account_id(p_provider text, p_workspace_id text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM oauth_tokens
    WHERE provider = p_provider
      AND provider_workspace_id = p_workspace_id
    LIMIT 1
$$;

-- ── 4. Reachable-graphs authorization backbone (permanent RLS carve-out) ────
-- NOT a bootstrap-deadlock case like (1)-(3) above — this is a DIFFERENT,
-- PERMANENT exception: `list_reachable_graphs`/`graph_reachable_by_user` back
-- ~13 route files' authorization checks with a deliberately MULTI-account
-- predicate (a user can belong to several `accounts` rows and see across all
-- of them in one query, per `middleware/auth.rs`'s documented "Model B"),
-- which cannot be expressed as a single `app.account_id` RLS scope. Rather
-- than RLS-scope this (impossible without changing product behavior) or
-- leave it un-backstopped, it is re-implemented here under `SECURITY
-- DEFINER`, re-running the IDENTICAL `account_members`/`team_members`
-- predicate the application-level query already ran before this PR — a bug
-- in the predicate is still caught by review/tests, it is just never
-- double-enforced by a Postgres RLS policy the way the other 36 tables are.
-- Founder-review flagged (see this migration's header) as a deliberate,
-- reviewed exception, not an oversight — do not "fix" this by adding
-- `begin_account_scoped` around its callers; that would not even compile
-- (there is no single scoping account_id for a multi-account read).
--
-- Returns the exact 5 columns `list_reachable_graphs` selects (no more) —
-- ORDER BY/LIMIT are baked in here too, matching the existing query
-- byte-for-byte, so the Rust-side caller becomes a trivial passthrough.
CREATE OR REPLACE FUNCTION resolve_reachable_graphs(p_user_id uuid)
RETURNS TABLE(id uuid, name text, kind text, is_primary boolean, created_at timestamptz)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.id, g.name, g.kind, g.is_primary, g.created_at
    FROM graphs g
    WHERE g.account_id IN
        (SELECT account_id FROM account_members WHERE user_id = p_user_id)
      AND (g.kind <> 'team' OR g.id IN
        (SELECT t.graph_id FROM teams t
         JOIN team_members tm ON tm.team_id = t.id
         WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
    ORDER BY g.is_primary DESC, g.created_at DESC
    LIMIT 500
$$;

-- Same predicate as (4) above, specialized to a single graph_id existence
-- check (the IDOR guard `graph_reachable_by_user` backs) rather than
-- transferring the full reachable set. Kept as a separate function (not
-- "call resolve_reachable_graphs and check membership client-side") so the
-- existing `EXISTS(...)` short-circuit's efficiency is preserved unchanged.
CREATE OR REPLACE FUNCTION resolve_graph_reachable_by_user(p_graph_id uuid, p_user_id uuid)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT EXISTS (
        SELECT 1 FROM graphs g
        WHERE g.id = p_graph_id
          AND g.account_id IN
              (SELECT account_id FROM account_members WHERE user_id = p_user_id)
          AND (g.kind <> 'team' OR g.id IN
              (SELECT t.graph_id FROM teams t
               JOIN team_members tm ON tm.team_id = t.id
               WHERE tm.user_id = p_user_id AND tm.departing_at IS NULL))
    )
$$;

-- ── Grants ────────────────────────────────────────────────────────────────
-- Postgres grants EXECUTE on newly-created functions to PUBLIC by default
-- (unlike tables/sequences) — load-bearing to close here, NOT a harmless
-- default (CodeRabbit review of this PR, MAJOR): these five functions are
-- `SECURITY DEFINER` and bypass RLS BY DESIGN (the whole point — a caller-
-- supplied `p_user_id`/`p_graph_id`, no session-derived check at all), so
-- leaving the PUBLIC default in place would let ANY role with mere `CONNECT`
-- on this database — not just `coralswarm_app` — call them and enumerate
-- another account's graph/oauth-workspace ownership, regardless of whether
-- that role has any table-level `SELECT` on `graphs`/`atoms`/`oauth_tokens`/
-- `account_members`/etc. at all. That is a live privilege-escalation surface
-- the moment this migration runs (e.g. a future reporting/migration-tooling
-- role that is granted `CONNECT` but deliberately no content-table access),
-- not merely a defense-in-depth nicety — REVOKE first, then GRANT back only
-- to the two roles that legitimately need this: `coralswarm_app` (the future
-- least-privilege app role, OPS-4) and `CURRENT_USER` (whichever role runs
-- this migration — today's owner/BYPASSRLS role, which needs it for this to
-- be a true no-op behavior change pre-OPS-4, since the app connects as that
-- role today).
REVOKE EXECUTE ON FUNCTION resolve_graph_account_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_atoms_account_id(uuid[]) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_oauth_account_id(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_graph_reachable_by_user(uuid, uuid) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION resolve_graph_account_id(uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_atoms_account_id(uuid[]) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_oauth_account_id(text, text) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_reachable_graphs(uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_graph_reachable_by_user(uuid, uuid) TO coralswarm_app, CURRENT_USER;
