-- 0016_anchor_mismatch_bootstrap_functions.sql
--
-- TENANT-TREE PAIR of control migration `0105_anchor_mismatch_bootstrap_functions`.
-- See that file for why these three resolvers exist and what bug they close.
--
-- ── WHY PAIRED RATHER THAN CONTROL-ONLY ──────────────────────────────────────
--
-- `conversations`, `relationship_edge` and `transcript_corrections` are all
-- SHARED content tables: none appears in `check_schema_parity.sh`'s
-- `CONTROL_TABLES`, and all three are created by this tree. A resolver that
-- existed only on the control side would therefore diverge the two schemas —
-- which is exactly how the parity gate caught the first draft of this change,
-- where these were registered as control-only.
--
-- Same call as 0084/0003 made for `resolve_graph_account_id` and
-- `resolve_atoms_account_id`: paired, with BYTE-IDENTICAL bodies. The bodies
-- below must stay character-for-character equal to their control twins —
-- `pg_dump` compares the normalised function source, so a whitespace-only edit
-- to one tree fails parity.
--
-- ── GRANTS ───────────────────────────────────────────────────────────────────
--
-- 0009_tenant_rls deliberately keeps an explicit signature allowlist and no
-- default-privileges entry, noting that "a future resolver 42501s on first use,
-- which is the correct outcome — it sends whoever added it back here to grant it
-- deliberately". This is that deliberate grant.
--
-- Guarded on the role existing: this tree is also applied to databases
-- provisioned before the NOBYPASSRLS tenant role, and to local scratch DBs in
-- the parity harness, where `coralswarm_tenant_app` is absent.
--
-- ── ROLLBACK ─────────────────────────────────────────────────────────────────
--   REVOKE EXECUTE ON FUNCTION resolve_conversation_account_id(uuid) FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION resolve_relationship_edge_account_id(uuid) FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION resolve_transcript_correction_account_id(uuid) FROM coralswarm_tenant_app;
--   DROP FUNCTION IF EXISTS resolve_conversation_account_id(uuid);
--   DROP FUNCTION IF EXISTS resolve_relationship_edge_account_id(uuid);
--   DROP FUNCTION IF EXISTS resolve_transcript_correction_account_id(uuid);

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

REVOKE EXECUTE ON FUNCTION resolve_conversation_account_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_relationship_edge_account_id(uuid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_transcript_correction_account_id(uuid) FROM PUBLIC;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
    GRANT EXECUTE ON FUNCTION resolve_conversation_account_id(uuid) TO coralswarm_tenant_app;
    GRANT EXECUTE ON FUNCTION resolve_relationship_edge_account_id(uuid) TO coralswarm_tenant_app;
    GRANT EXECUTE ON FUNCTION resolve_transcript_correction_account_id(uuid) TO coralswarm_tenant_app;
  END IF;
END $$;
