-- Migration: 0046_reef_judgement — tenant twin of migrations_control/0139_reef_judgement.sql.
-- See that file's header for full rationale (bare node-id columns, CREATE-before-ALTER order,
-- graph_id-anchored RLS, and the note that this PR ships schema + Store plumbing ONLY — no
-- worker/route caller yet, no feature flag needed). Depends on: 0001_tenant_baseline (nodes,
-- graphs, node_merges), 0009_tenant_rls.
--
-- Review round 1 (see control twin's header for the full note): DDL edits are the two index
-- changes below (findings 9/10) — `idx_node_merges_judgement_id`/`idx_reef_judgement_superseded_by`
-- added, bare `idx_reef_judgement_node_a` dropped. Everything else review round 1 fixed lives
-- in `Store`, not here.
-- Down:
--   ALTER TABLE node_merges DROP COLUMN IF EXISTS judgement_id;
--   DROP POLICY IF EXISTS member_ocean_isolation ON reef_judgement;
--   ALTER TABLE reef_judgement DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS reef_judgement;

CREATE TABLE IF NOT EXISTS reef_judgement (
    judgement_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id       UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    node_a_id      UUID        NOT NULL,
    node_b_id      UUID        NOT NULL,

    verdict        TEXT        NOT NULL CHECK (verdict IN ('positive', 'negative', 'unsure')),
    source         TEXT        NOT NULL CHECK (source <> ''),
    confidence     DOUBLE PRECISION,
    model_id       TEXT,
    prompt_version TEXT,
    evidence       JSONB       NOT NULL DEFAULT '{}'::jsonb,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by     UUID,  -- bare uuid, no FK.

    expires_at     TIMESTAMPTZ,

    annulled_at    TIMESTAMPTZ,
    annulled_by    UUID,  -- bare uuid, no FK.
    annulled_by_kind TEXT CHECK (annulled_by_kind IN ('human', 'system')),  -- Q1, see control twin.
    annul_reason   TEXT,
    superseded_by  UUID        REFERENCES reef_judgement(judgement_id),

    CONSTRAINT reef_judgement_ordered_pair CHECK (node_a_id < node_b_id),
    CONSTRAINT reef_judgement_expiry_shape CHECK (verdict = 'unsure' OR expires_at IS NULL),
    CONSTRAINT reef_judgement_annul_shape  CHECK (annulled_by IS NULL OR annulled_at IS NOT NULL),
    CONSTRAINT reef_judgement_annul_kind_shape CHECK ((annulled_at IS NULL) = (annulled_by_kind IS NULL))
);

ALTER TABLE node_merges
    ADD COLUMN IF NOT EXISTS judgement_id UUID
        REFERENCES reef_judgement(judgement_id) ON DELETE SET NULL;

-- FK-covering indexes (review round 1, finding 9) — see control twin for the full rationale.
CREATE INDEX IF NOT EXISTS idx_node_merges_judgement_id
    ON node_merges (judgement_id) WHERE judgement_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_reef_judgement_superseded_by
    ON reef_judgement (superseded_by) WHERE superseded_by IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_reef_judgement_active_pair
    ON reef_judgement (graph_id, node_a_id, node_b_id) WHERE annulled_at IS NULL;
-- review round 1, finding 10: bare `idx_reef_judgement_node_a` dropped — redundant with
-- `uq_reef_judgement_active_pair`'s leading `(graph_id, node_a_id, ...)` columns. See
-- control twin for the full rationale.
CREATE INDEX IF NOT EXISTS idx_reef_judgement_node_b
    ON reef_judgement (node_b_id) WHERE annulled_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_reef_judgement_review_queue
    ON reef_judgement (graph_id, created_at) WHERE annulled_at IS NULL AND verdict = 'unsure';
CREATE INDEX IF NOT EXISTS idx_reef_judgement_pending_positive
    ON reef_judgement (graph_id, created_at) WHERE annulled_at IS NULL AND verdict = 'positive';
CREATE INDEX IF NOT EXISTS idx_reef_judgement_pair_history
    ON reef_judgement (graph_id, node_a_id, node_b_id, created_at DESC);

ALTER TABLE reef_judgement ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON reef_judgement;
CREATE POLICY member_ocean_isolation ON reef_judgement
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
