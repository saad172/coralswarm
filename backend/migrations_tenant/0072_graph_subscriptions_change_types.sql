-- Migration: 0072_graph_subscriptions_change_types
-- Description: m365 enterprise train PR 4.4 — schema-parity twin of
--   `migrations_control/0195_graph_subscriptions_change_types.sql`. See that
--   migration's header for the full rationale (a pre-4.4 subscription is
--   renewed by `PATCH` forever and so never widens its `changeType`, meaning
--   deletion propagation would silently never fire for any existing
--   install). Byte-identical schema change; nothing to restate here — this
--   column carries no FK on either tree, and `graph_subscriptions` is
--   deliberately NOT RLS-covered in this tree (the call
--   `migrations_tenant/0009_tenant_rls.sql` already made: no `graph_id` to
--   anchor a policy against), so no policy changes ride along.
--
-- Down: ALTER TABLE graph_subscriptions DROP COLUMN IF EXISTS change_types;

ALTER TABLE graph_subscriptions
    ADD COLUMN IF NOT EXISTS change_types TEXT NOT NULL DEFAULT 'created';
