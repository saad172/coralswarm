-- Migration: 0127_review_deadlines
-- Description: `relationship_edge.queued_at` — when the edge last entered the
--   review queue (transitioned INTO status='suggested'). The zero-debt half
--   of high-signal review: every queued clarification carries a deadline
--   (`review_policy::REVIEW_DEADLINE_DAYS`, 7d), after which the
--   `review_deadline` sweep applies the machine's best guess (confirm at/
--   above the traversability floor, else blank into `evaluation_ledger`), so
--   the review badge is structurally incapable of unbounded growth.
--
--   Why a dedicated column: `updated_at` is bumped by the upsert on EVERY
--   re-inference pass even when nothing changed, so an updated_at-based
--   deadline could be starved forever; `created_at` misfires for an edge that
--   was auto-confirmed and later machine-downgraded back to suggested (it
--   would look instantly expired). Nullable, no default, no table rewrite;
--   maintained by the upsert (set on transition into suggested, kept
--   otherwise — a stale stamp on a non-suggested row is harmless because the
--   sweep filters status='suggested').
--
--   Deliberately NOT added to the sync op schema (`sync::relationship_edge_op`):
--   deadline resolution is a cloud-plane behavior; the desktop replica never
--   reads the column.
--
--   Pending DECISIONS get the JSON equivalent (`body.pending_since`, stamped
--   by the supersession flip sites) — no DDL needed there.
--
-- Depends on: 0040_relationship_edges (relationship_edge)
-- Down:
--   ALTER TABLE relationship_edge DROP COLUMN IF EXISTS queued_at;

ALTER TABLE relationship_edge
  ADD COLUMN IF NOT EXISTS queued_at TIMESTAMPTZ;

-- Backfill: rows already sitting in the queue start their deadline clock at
-- their creation (the closest honest anchor that predates this column).
UPDATE relationship_edge
   SET queued_at = created_at
 WHERE status = 'suggested'
   AND queued_at IS NULL;
