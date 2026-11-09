-- 0105_anchor_mismatch_bootstrap_functions.sql
--
-- Three `resource_id -> account_id` bootstrap resolvers, closing the anchor
-- mismatch that makes a legitimate owner receive a 404 on their own data.
--
-- ── THE BUG THESE CLOSE ──────────────────────────────────────────────────────
--
-- `routes/nodes.rs` and `routes/meetings.rs` both state the rule: GRAPH-anchored
-- handlers scope to the OCEAN'S OWNING account (reachability is multi-account by
-- design, ADR-0007 Model B), while ID-anchored handlers scope to the CALLER'S
-- ACTIVE account — "the only account knowable before the row is read".
--
-- That second clause is a limitation, not a preference. It holds only because no
-- resolver exists for these three resources. Where one DOES exist the handler is
-- expected to use it: `services/promotion.rs` already calls
-- `resolve_node_account_id` (0086) for exactly this reason.
--
-- The consequence, reproduced against a real dataset before writing this file:
-- one conversation, one user, one database — visible when scoped to the ocean's
-- owning account (1 row, what the graph-anchored LIST route does) and invisible
-- when scoped to the caller's other active account (0 rows, what the id-anchored
-- MUTATE route does). The sidebar renders the row; opening, renaming or deleting
-- it 404s. The same shape hits relationship confirm/reject and transcript
-- correction revert/restore.
--
-- The precondition is ordinary, not exotic: any user who belongs to more than one
-- account — every org member, and anyone using the Personal/Org switcher — can
-- have an active account that differs from the ocean's owner.
--
-- ── WHY SECURITY DEFINER, AND WHAT THAT COSTS ────────────────────────────────
--
-- Same reasoning as 0084/0086: the account is what we are trying to DISCOVER, so
-- the lookup cannot itself be scoped by it. Each function therefore returns a
-- BARE account id and nothing else — no titles, no bodies, no membership facts.
-- Knowing that a uuid you already hold belongs to account X leaks nothing the
-- caller's own reachability check will not immediately re-verify.
--
-- RLS is NOT evaluated inside a SECURITY DEFINER body, so each function below is
-- deliberately a single narrow SELECT with no joins beyond the graph it needs to
-- reach `account_id`, is STABLE (never mutating), pins `search_path`, and is
-- REVOKEd from PUBLIC before being GRANTed to the app role.
--
-- The caller remains responsible for authorization. These answer only "which
-- account owns this row"; the handler must still prove the caller may reach it.
--
-- ── ROLLBACK ─────────────────────────────────────────────────────────────────
--   REVOKE EXECUTE ON FUNCTION resolve_conversation_account_id(uuid) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_relationship_edge_account_id(uuid) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_transcript_correction_account_id(uuid) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_conversation_account_id(uuid);
--   DROP FUNCTION IF EXISTS resolve_relationship_edge_account_id(uuid);
--   DROP FUNCTION IF EXISTS resolve_transcript_correction_account_id(uuid);

-- ── 1. conversation_id -> account_id ─────────────────────────────────────────
-- `conversations` carries `account_id` directly (stamped at creation from the
-- primary graph's owner), so this needs no join at all.
CREATE OR REPLACE FUNCTION resolve_conversation_account_id(p_conversation_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT c.account_id
    FROM conversations c
    WHERE c.id = p_conversation_id
$$;

-- ── 2. relationship_edge id -> account_id ────────────────────────────────────
-- Same shape as 0086's `resolve_node_account_id`, one hop through `graphs`.
CREATE OR REPLACE FUNCTION resolve_relationship_edge_account_id(p_edge_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.account_id
    FROM relationship_edge e
    JOIN graphs g ON g.id = e.graph_id
    WHERE e.edge_id = p_edge_id
$$;

-- ── 3. transcript_correction id -> account_id ────────────────────────────────
-- `meetings.rs`'s module doc names this function by the shape it would need,
-- records the resulting gap as "recorded here rather than silently accepted",
-- and it was then never built. This is that function.
CREATE OR REPLACE FUNCTION resolve_transcript_correction_account_id(p_correction_id uuid)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.account_id
    FROM transcript_corrections tc
    JOIN graphs g ON g.id = tc.graph_id
    WHERE tc.correction_id = p_correction_id
$$;

-- ── Grants ───────────────────────────────────────────────────────────────────
-- REVOKE FROM PUBLIC first, then grant narrowly. 0087 shipped three definers
-- with no revokes and 0089 had to close it; that is why this block is not
-- optional. Granted to `coralswarm_app` and CURRENT_USER, matching 0084/0086.
-- The TENANT-plane grant lives in this migration's pair,
-- `migrations_tenant/0016_anchor_mismatch_bootstrap_functions.sql` — these are
-- shared content tables, so both trees define the functions with byte-identical
-- bodies and each grants its own plane's role.
REVOKE EXECUTE ON FUNCTION resolve_conversation_account_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_relationship_edge_account_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_transcript_correction_account_id(uuid) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION resolve_conversation_account_id(uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_relationship_edge_account_id(uuid) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_transcript_correction_account_id(uuid) TO coralswarm_app, CURRENT_USER;
