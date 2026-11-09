-- Migration: 0045_reef_identifiers
-- Description: The TENANT half of control's `0138_reef_identifiers` — see that
--   migration's header for the full rationale (near-decisive entity-
--   resolution identity signals: email/Slack-user/git-author/Clerk-user,
--   plus the extraction-time "attach, don't duplicate" lookup). DDL kept
--   byte-equivalent.
--
-- PAIRED WITH: migrations_control/0138_reef_identifiers.sql
-- Depends on: 0001_tenant_baseline (nodes, graphs), 0009_tenant_rls (RLS
--             pattern this follows).
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON reef_identifiers;
--   ALTER TABLE reef_identifiers DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS reef_identifiers;
--
-- RLS here is the TENANT-plane mirror of 0138's `account_isolation` policy —
-- same one-endpoint (`node_id`) anchor, different predicate
-- (`tenant_rls_visible_graph_ids()` instead of `current_setting('app.account_id')`)
-- because this plane's isolation boundary is the reachable-graph set, not an
-- account id — same `conflict_dismissal`/0023 precedent.

CREATE TABLE IF NOT EXISTS reef_identifiers (
    graph_id   UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    node_id    UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    kind       TEXT        NOT NULL CHECK (kind IN ('email', 'slack_user', 'git_author', 'clerk_user')),
    -- Caller-normalized identity value — see 0138's header.
    value_norm TEXT        NOT NULL CHECK (value_norm <> ''),
    -- Free-text provenance, e.g. 'extraction', 'connector_identity_match',
    -- 'backfill' — see 0138's header.
    source     TEXT        NOT NULL CHECK (source <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (graph_id, kind, value_norm)
);

CREATE INDEX IF NOT EXISTS idx_reef_identifiers_node ON reef_identifiers (node_id);

ALTER TABLE reef_identifiers ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON reef_identifiers;
CREATE POLICY member_ocean_isolation ON reef_identifiers
    USING (node_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));
