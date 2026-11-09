-- Migration: 0138_reef_identifiers
-- Description: `reef_identifiers` (slice 0, T2 "identifier awareness") — a
--   durable, per-graph directory of STRONG identity signals attached to an
--   entity reef: an email address, a Slack user id, a git commit author
--   string, or a Clerk user id. These are near-decisive evidence for
--   entity-resolution (`workers::entity_resolution`): two reefs sharing an
--   `email`/`slack_user`/`clerk_user` identifier are almost certainly the
--   same real-world person, which is a much stronger signal than name
--   similarity alone (see that worker's `resolve_entities_v1.txt` adjudication
--   prompt, which now surfaces identifier facts).
--
--   Also used at extraction time (`workers::extract`) to avoid spinning up a
--   second bare-email-titled reef when one already carries that identifier —
--   a new mention of an already-known email attaches to the existing reef
--   instead of creating a duplicate.
--
-- PAIRED WITH: migrations_tenant/0045_reef_identifiers.sql — the table DDL in
--              the two files is IDENTICAL (`scripts/check_schema_parity.sh`);
--              the RLS layered on top of it deliberately is NOT (see below;
--              same asymmetry as every other shared content table since
--              0083/0009 diverged — `conflict_dismissal`'s migrations are the
--              most recent precedent for this exact split).
-- Depends on: 0011_nodes (nodes, graphs), 0083_consumer_rls (RLS pattern).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON reef_identifiers;
--   ALTER TABLE reef_identifiers DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS reef_identifiers;
--
-- `graph_id` is denormalized alongside `node_id` (rather than requiring every
-- reader to join through `nodes`) so the `UNIQUE (graph_id, kind, value_norm)`
-- constraint — "this email/slack id/etc. names exactly one reef per ocean" —
-- can be enforced directly by Postgres without a trigger, and so the
-- extraction-time "does a reef already own this identifier?" lookup
-- (`Store::find_node_id_by_identifier`) is a single index-only scan. A future
-- `nodes.graph_id` UPDATE (there is none today — reefs do not move oceans)
-- would need to carry this column along; flagged here rather than silently
-- assumed.
--
-- `value_norm` is the CALLER's normalized form (lowercased/trimmed email,
-- Slack's own opaque user id, etc.) — this table does no normalization of its
-- own, so two callers must agree on the same normalization for the same
-- identity, exactly like `nodes`'s own case-insensitive-identity precedent
-- (migration 0107) pushes normalization to the write path rather than the
-- schema.
--
-- No `updated_at`/`version` and no oplog entry: unlike reefs/edges, an
-- identifier is either present or it is not (no in-place mutation — a
-- superseded identifier is deleted and a fresh one inserted), and it is
-- purely server-side resolution bookkeeping with no desktop/libsql equivalent
-- (same "cloud-only, no sync op" class as `graphs.er_sweep_cursor_*`,
-- migration 0136 — see that migration's header for the precedent).
--
-- RLS: same reasoning as `conflict_dismissal` (0112's own header) — under
-- OPS-4's `coralswarm_app` role, an RLS-covered table with no policy is
-- invisible (hard error), not merely unfiltered, which is the desired failure
-- mode for a table reached exclusively through `Store`. One-endpoint anchor
-- (`node_id` — there is only one per row here, unlike `conflict_dismissal`'s
-- two), same `nodes -> graphs -> account_id` hop.
CREATE TABLE IF NOT EXISTS reef_identifiers (
    graph_id   UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    node_id    UUID        NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    kind       TEXT        NOT NULL CHECK (kind IN ('email', 'slack_user', 'git_author', 'clerk_user')),
    -- Caller-normalized identity value (see header) — never re-normalized here.
    value_norm TEXT        NOT NULL CHECK (value_norm <> ''),
    -- Free-text provenance, e.g. 'extraction', 'connector_identity_match',
    -- 'backfill' (scripts/backfill_email_reef_identifiers.rs) — auditability,
    -- not a gating predicate.
    source     TEXT        NOT NULL CHECK (source <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (graph_id, kind, value_norm)
);

CREATE INDEX IF NOT EXISTS idx_reef_identifiers_node ON reef_identifiers (node_id);

ALTER TABLE reef_identifiers ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON reef_identifiers;
CREATE POLICY account_isolation ON reef_identifiers
    USING (node_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    ));
