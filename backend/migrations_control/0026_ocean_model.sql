-- Migration: 0026_ocean_model
-- Description: ADR-0006 ocean model. Widen graphs.kind to include 'org', add a
--              per-account 'is_primary' designation, enforce the cardinality
--              rules (one primary + one org ocean per account), and add the
--              node_promotions audit table for one-way copy-on-promote.
--
-- Depends on: 0003_atoms (graphs), 0011_nodes (nodes), 0022_teams (graphs.kind)
--
-- Down:
--   DROP TABLE IF EXISTS node_promotions;
--   DROP INDEX IF EXISTS uq_graphs_one_org_per_account;
--   DROP INDEX IF EXISTS uq_graphs_one_primary_per_account;
--   ALTER TABLE graphs DROP COLUMN IF EXISTS is_primary;
--   ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_kind_check;
--   ALTER TABLE graphs ADD CONSTRAINT graphs_kind_check CHECK (kind IN ('personal','team'));

-- ── graphs.kind: allow 'org' ───────────────────────────────────────────────────
-- 0022 added kind with an inline CHECK (auto-named graphs_kind_check) restricting
-- it to ('personal','team'). Widen it to the three ocean levels (ADR-0006).
ALTER TABLE graphs DROP CONSTRAINT IF EXISTS graphs_kind_check;
ALTER TABLE graphs
    ADD CONSTRAINT graphs_kind_check CHECK (kind IN ('personal', 'team', 'org'));

-- ── graphs.is_primary ──────────────────────────────────────────────────────────
-- Exactly one personal ocean per account is the user's PRIMARY (private) ocean —
-- the default destination for ingest and the authoritative origin for promotion.
ALTER TABLE graphs
    ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT false;

-- Cardinality: at most one primary ocean per account.
CREATE UNIQUE INDEX IF NOT EXISTS uq_graphs_one_primary_per_account
    ON graphs (account_id)
    WHERE is_primary;

-- Cardinality: at most one organization ocean per account (the org commons).
CREATE UNIQUE INDEX IF NOT EXISTS uq_graphs_one_org_per_account
    ON graphs (account_id)
    WHERE kind = 'org';

-- ── node_promotions ────────────────────────────────────────────────────────────
-- Provenance for one-way copy-on-promote (ADR-0006). A promotion publishes a COPY
-- of a source reef (which stays authoritative in its origin ocean) into a wider
-- shared ocean's graph as target_node_id, recording who promoted it and from
-- which source version. Re-promoting the same source into the same target updates
-- the existing copy (re-publish), enforced by the (source, target) unique key.
CREATE TABLE IF NOT EXISTS node_promotions (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_node_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    target_graph_id UUID        NOT NULL REFERENCES graphs(id)     ON DELETE CASCADE,
    target_node_id  UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    promoted_by     UUID        REFERENCES users(id) ON DELETE SET NULL,
    from_version    INTEGER     NOT NULL,
    promoted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One published copy per (source reef, target ocean): re-promote updates it.
    UNIQUE (source_node_id, target_graph_id)
);

CREATE INDEX IF NOT EXISTS idx_node_promotions_target
    ON node_promotions (target_graph_id);

CREATE INDEX IF NOT EXISTS idx_node_promotions_source
    ON node_promotions (source_node_id);
