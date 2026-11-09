-- Migration: 0023_conflict_dismissal
-- Description: `conflict_dismissal` — a durable negative marker for §3.4's
--   conflict-detection worker, so a pair the LLM has already adjudicated as
--   "different" or "unsure" is not re-blocked, re-ranked, and re-sent to
--   Haiku on every subsequent debounced pass.
-- PAIRED WITH: migrations_control/0112_conflict_dismissal.sql — the table DDL
--              in the two files is IDENTICAL (`scripts/check_schema_parity.sh`);
--              the RLS layered on top of it deliberately is NOT (see below and
--              see 0112's own header for the full design rationale — expiry
--              policy, distinctness from `non_supersession_pair`, prompt-
--              version invalidation, and auditability — which is NOT repeated
--              here to avoid the two copies drifting out of sync in prose
--              while the DDL itself is kept byte-identical).
-- Depends on: 0001_tenant_baseline (nodes), 0009_tenant_rls (RLS pattern this follows).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON conflict_dismissal;
--   ALTER TABLE conflict_dismissal DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS conflict_dismissal;
--
-- RLS here is the TENANT-plane mirror of 0112's `account_isolation` policy —
-- same `non_supersession_pair` precedent (0009's own section), same one-
-- endpoint anchor (`node_a_id`), same "the write path only ever proposes
-- same-graph pairs, an invariant `workers::conflict_detection` maintains in
-- code" caveat, different predicate (`tenant_rls_visible_graph_ids()` instead
-- of `current_setting('app.account_id')`) because this plane's isolation
-- boundary is the reachable-graph set, not an account id. Registered in
-- `scripts/check_schema_parity.sh`'s `TENANT_ONLY_RLS_TABLES` (alongside
-- `non_supersession_pair`) in this same PR.

CREATE TABLE IF NOT EXISTS conflict_dismissal (
    node_a_id      UUID             NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    node_b_id      UUID             NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    -- 'different': both claims are true and coexist (permanent — never
    -- expires by time). 'unsure': the model could not tell from the text
    -- alone (expires — see expires_at). Deliberately excludes 'conflicts':
    -- a confirmed conflict WRITES a supersession_edge (§3.4/§3.5); it has
    -- nothing to be "dismissed" from.
    verdict        TEXT             NOT NULL CHECK (verdict IN ('different', 'unsure')),
    -- The adjudication model id (e.g. `claude-haiku-4-5`) — whatever
    -- `llm::model_config::conflict_model()` resolved to at write time.
    model          TEXT             NOT NULL CHECK (model <> ''),
    -- The adjudication PROMPT version (e.g. `detect_conflicts_v1`, derived
    -- from `prompts/detect_conflicts_v1.txt`'s own filename) — see 0112's
    -- header for why this re-gates on a prompt change.
    prompt_version TEXT             NOT NULL CHECK (prompt_version <> ''),
    -- The model's own confidence in `verdict`, carried for free from the
    -- same adjudication call — auditability, not a gating predicate.
    confidence     DOUBLE PRECISION NOT NULL DEFAULT 0,
    dismissed_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    -- NULL = never expires (always true for 'different'). Set for 'unsure'
    -- to `dismissed_at + CONFLICT_UNSURE_DISMISSAL_TTL_DAYS` at write time —
    -- see 0112's header for the policy and the query-time-only enforcement.
    expires_at     TIMESTAMPTZ,

    -- Order-insensitive uniqueness: callers must always insert with
    -- node_a_id < node_b_id (mirrors non_supersession_pair's own constraint
    -- and rationale verbatim).
    CONSTRAINT conflict_dismissal_ordered CHECK (node_a_id < node_b_id),
    -- Pins the expiry policy into the schema: an 'unsure' row MAY carry an
    -- expiry, a 'different' row MUST NOT (see 0112's header). Catches a
    -- future writer that forgets the asymmetry at INSERT time rather than
    -- letting a 'different' row silently start expiring.
    CONSTRAINT conflict_dismissal_expiry_matches_verdict
        CHECK (verdict = 'unsure' OR expires_at IS NULL),
    UNIQUE (node_a_id, node_b_id)
);

ALTER TABLE conflict_dismissal ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON conflict_dismissal;
CREATE POLICY member_ocean_isolation ON conflict_dismissal
    USING (node_a_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));
