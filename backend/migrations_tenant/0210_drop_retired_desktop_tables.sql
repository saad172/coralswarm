-- Migration: 0210_drop_retired_desktop_tables
-- PAIRED WITH: control 0395 (drop_retired_desktop_tables).
-- contract: safe-after server-2026.10.29.5
--
-- The desktop app is retired (ADR 0015). sync_cursors (0001 baseline) held
-- the desktop<->cloud oplog sync cursor; no tenant code reads or writes it.
-- No RLS, no foreign keys, no index beyond its primary key.

DROP TABLE IF EXISTS sync_cursors;
