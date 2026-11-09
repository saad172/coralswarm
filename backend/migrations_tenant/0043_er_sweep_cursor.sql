-- Migration: 0043_er_sweep_cursor
-- Description: The TENANT half of control's `0136_er_sweep_cursor` — see that
--   migration's header for the full rationale (the entity-resolution
--   keyset-pagination sweep cursor that replaces the old fixed 500-entity-reef
--   cap). DDL kept byte-equivalent; no RLS change needed (`graphs` is already
--   fully covered by `member_ocean_isolation` on this tree — see
--   0009_tenant_rls).
--
-- PAIRED WITH: migrations_control/0136_er_sweep_cursor.sql
-- Depends on: 0001_tenant_baseline (graphs)
-- Down:
--   ALTER TABLE graphs
--       DROP COLUMN IF EXISTS er_sweep_cursor_updated_at,
--       DROP COLUMN IF EXISTS er_sweep_cursor_node_id;

ALTER TABLE graphs
    ADD COLUMN IF NOT EXISTS er_sweep_cursor_updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS er_sweep_cursor_node_id UUID;
