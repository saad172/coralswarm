-- Migration: 0034_review_deadlines
-- Description: The TENANT half of control's `0127_review_deadlines` — see that
--   migration's header for the full rationale (`relationship_edge.queued_at`,
--   the zero-debt review deadline anchor: why it is a dedicated column rather
--   than `updated_at`/`created_at`, why it is nullable with no default, and
--   why it deliberately stays out of the sync op schema). DDL kept
--   byte-equivalent.
--
-- PAIRED WITH: migrations_control/0127_review_deadlines.sql
-- Depends on: 0001_tenant_baseline (relationship_edge)
-- Down:
--   ALTER TABLE relationship_edge DROP COLUMN IF EXISTS queued_at;

ALTER TABLE relationship_edge
  ADD COLUMN IF NOT EXISTS queued_at TIMESTAMPTZ;

UPDATE relationship_edge
   SET queued_at = created_at
 WHERE status = 'suggested'
   AND queued_at IS NULL;
