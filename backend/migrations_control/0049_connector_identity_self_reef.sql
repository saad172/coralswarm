-- Migration: 0049_connector_identity_self_reef
-- Description: Identity-resolution substrate for stated-relationship extraction
--              (CS-ER). Two complementary pieces that make every authored atom
--              from the same human anchor to ONE author reef:
--
--   1. `connector_identities` — a connector-side identity directory keyed by
--      (provider, workspace_id, external_user_id) → canonical_name. The same
--      human currently surfaces under different `atoms.author_name` values (a
--      Slack display name in one atom, a raw user id like "U57C3A9EF09" in
--      another) because the per-process name cache misses across restarts and
--      falls back to the raw id. Persisting the connector's id→name mapping here
--      gives a STABLE canonical name, so "my wife" and "my daughter" authored by
--      the same person resolve to a single reef. Populated from connector data
--      (Slack `users.info`) at ingest; consulted before re-deriving.
--
--      Server-only (cloud/Postgres). NOT in the libsql desktop bootstrap SCHEMA
--      and NOT synced via the oplog: it is connector ingest infrastructure that
--      only runs on the Postgres backend (the message pipeline + `users.info`
--      lookups live cloud-side), exactly like `slack_integrations` (mig 0015)
--      and `oauth_tokens`. The reefs/edges it influences ARE synced through the
--      normal node/relationship ops; this directory itself is a local lookup.
--
--   2. `graphs.self_node_id` — a single designated "self" person reef per ocean
--      representing the ocean owner. First-person ("I"/"my X") authored by the
--      ocean owner binds to this one reef even when the owner's display name
--      varies across sources. Lazily created on first owner-authored ingest into
--      the primary ocean (see `Store::ensure_self_node`). FK to nodes with
--      ON DELETE SET NULL so deleting the reef (e.g. via merge) clears the
--      pointer rather than cascading the ocean away; the reef itself, being a
--      normal `nodes` row, syncs through the existing node-upsert op.
--
-- Depends on: 0001_init (accounts), 0003_atoms (graphs), 0011_nodes (nodes),
--             0015_slack_integrations (style reference)
-- Down:
--   ALTER TABLE graphs DROP COLUMN IF EXISTS self_node_id;
--   DROP TABLE IF EXISTS connector_identities;

-- ── 1. Connector identity directory (id → canonical display name) ─────────────
CREATE TABLE IF NOT EXISTS connector_identities (
    -- Connector id matching `oauth_tokens.provider` / the `:provider` segment
    -- (e.g. 'slack', 'microsoft').
    provider          TEXT        NOT NULL,
    -- Provider-native workspace/tenant id (Slack team id "T…", MS tenant id).
    workspace_id      TEXT        NOT NULL,
    -- Provider-native user id (Slack "U…", MS `oid`) — the STABLE identifier the
    -- connector knows, independent of any display-name quirk.
    external_user_id  TEXT        NOT NULL,
    -- The single canonical display name all this human's authored atoms use.
    canonical_name    TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (provider, workspace_id, external_user_id)
);

-- ── 2. Per-ocean "self" reef pointer ──────────────────────────────────────────
-- One owner-person reef per ocean; first-person edges anchor here. SET NULL on
-- the reef's deletion so a self-reef merge/delete doesn't cascade-drop the ocean.
ALTER TABLE graphs
    ADD COLUMN IF NOT EXISTS self_node_id UUID
        REFERENCES nodes(node_id) ON DELETE SET NULL;
