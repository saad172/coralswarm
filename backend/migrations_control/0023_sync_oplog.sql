-- Migration: 0023_sync_oplog
-- Description: Desktop-canonical bidirectional sync (ADR-0005). An append-only
--              per-store operation log (the cursor source of truth) plus the
--              sync cursors a peer keeps about each remote. The cloud is one
--              logical device ('cloud') sharing this one op_log across instances.
--
-- Depends on: 0003_atoms (graphs), 0011_nodes
--
-- Down:
--   DROP TABLE IF EXISTS sync_cursors;
--   DROP TABLE IF EXISTS op_log;

-- ── op_log ────────────────────────────────────────────────────────────────────
-- One row per mutation to a syncable entity (atom / node / node_atom /
-- supersession_edge). `seq` is the monotonic local cursor; `op_id` is the op's
-- global identity (idempotency key) — preserved when an op is relayed through
-- this store so the same logical op is never applied twice. `payload` is the
-- full-row snapshot for an upsert, or the last-known snapshot for a delete
-- tombstone (so the 30-day trash can restore it). `version` mirrors
-- `nodes.version` for reef last-write-wins; it is 0 for append-only entities.
CREATE TABLE op_log (
    seq        BIGSERIAL PRIMARY KEY,
    op_id      UUID NOT NULL UNIQUE,
    entity     TEXT NOT NULL,
    entity_id  TEXT NOT NULL,
    op_type    TEXT NOT NULL CHECK (op_type IN ('upsert', 'delete')),
    payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
    version    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL,
    device_id  TEXT NOT NULL,
    graph_id   UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Latest-op lookup per entity drives the last-write-wins / resurrection guard
-- in `apply_remote_op` (newest op for an entity_id, by seq).
CREATE INDEX idx_op_log_entity ON op_log (entity, entity_id, seq DESC);
-- Pulls scan `seq > cursor`, optionally filtered by origin device.
CREATE INDEX idx_op_log_seq_device ON op_log (seq, device_id);
-- Tombstone purge scans deletes by age.
CREATE INDEX idx_op_log_tombstones ON op_log (op_type, updated_at) WHERE op_type = 'delete';

-- ── sync_cursors ────────────────────────────────────────────────────────────────
-- What this store has synced with each peer. `peer` is a free-form key
-- (e.g. 'cloud' for the desktop→cloud pull cursor). Holds the highest remote
-- `seq` already applied. The cloud is peer-stateless (device-driven sync), so in
-- practice this table is populated on the desktop side.
CREATE TABLE sync_cursors (
    peer       TEXT PRIMARY KEY,
    cursor     BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
