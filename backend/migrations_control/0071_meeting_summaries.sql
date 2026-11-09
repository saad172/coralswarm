-- Migration: 0071_meeting_summaries
-- Description: Automatic post-meeting summary (overview + decisions + action
--              items, deep-linked to transcript timestamps) on the meeting row.
--
--   Extends `meeting_transcripts` (one row per ingested meeting source, keyed
--   `(graph_id, source_id)`) with:
--     - `segments`       : the timestamped transcript segments passed at ingest
--                          ({speaker,start_ms,end_ms,text}[]), used to anchor each
--                          decision/action-item to a transcript timestamp and to
--                          render mm:ss rows. Nullable — meetings ingested before
--                          this feature (and sources with no per-segment timing)
--                          have none, and their summary degrades to null ts_ms.
--     - `summary`        : the structured summary JSON produced by the async job
--                          ({overview, decisions:[{text,ts_ms}],
--                           action_items:[{text,owner,ts_ms}]}). Null until done.
--     - `summary_status` : the async job's client-pollable lifecycle. `blocked`
--                          is the Local-only egress terminal state that awaits a
--                          per-meeting cloud opt-in (never a silent cloud call).
--     - `summary_model`  : provenance — which model produced the reduce pass.
--     - `summary_error`  : last failure message (never transcript content),
--                          surfaced with the `failed` state's Retry affordance.
--     - `summary_updated_at` : app-managed touch time for the summary columns.
--
--   The verbatim transcript in `raw_blob_key` / `raw_inline` is NEVER touched —
--   the summary is derived, additive metadata on the same row. Execution + retry
--   run through the existing `jobs` queue (JobKind::GenerateMeetingSummary); these
--   columns are the client-facing status the meetings API returns and polls.
--
-- Depends on: 0056_meeting_transcripts (meeting_transcripts), 0004_jobs (queue).
-- Down:
--   ALTER TABLE meeting_transcripts
--     DROP COLUMN IF EXISTS segments,
--     DROP COLUMN IF EXISTS summary,
--     DROP COLUMN IF EXISTS summary_status,
--     DROP COLUMN IF EXISTS summary_model,
--     DROP COLUMN IF EXISTS summary_error,
--     DROP COLUMN IF EXISTS summary_updated_at;

ALTER TABLE meeting_transcripts
    ADD COLUMN IF NOT EXISTS segments           JSONB,
    ADD COLUMN IF NOT EXISTS summary            JSONB,
    ADD COLUMN IF NOT EXISTS summary_status     TEXT        NOT NULL DEFAULT 'pending'
                                                CHECK (summary_status IN
                                                    ('pending','running','done','failed','blocked')),
    ADD COLUMN IF NOT EXISTS summary_model      TEXT,
    ADD COLUMN IF NOT EXISTS summary_error      TEXT,
    ADD COLUMN IF NOT EXISTS summary_updated_at TIMESTAMPTZ;
